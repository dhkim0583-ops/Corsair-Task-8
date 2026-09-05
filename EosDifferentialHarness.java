/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration.differential;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.TrackingStateRestoreListener;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;

/**
 * Differential test harness for stateful processing under {@code exactly_once_v2}.
 *
 * <p>The harness runs the same seeded {@link DifferentialWorkload} through the same stateful topology twice:
 * once uninterrupted (the baseline) and once while a {@link FailureSchedule} crashes and starts instances in
 * the middle of processing. Both runs use fresh application ids and topics, so the only difference between them
 * is the failure schedule. After each run the harness reads the committed output with a {@code read_committed}
 * consumer and records, per output topic and per key, the ordered sequence of emitted values and timestamps.
 * {@link #compare(RunResult, RunResult)} then reports every key whose committed sequence differs.
 *
 * <p>The topology exercises three kinds of state with different time sensitivity:
 * <ul>
 *   <li>a processor-API key-value store that keeps a running count/sum/maximum-timestamp/out-of-order count
 *       per key and emits the state after every record;</li>
 *   <li>a tumbling-window sum with a grace period that emits an update on every record (caching disabled);</li>
 *   <li>the same windowed sum suppressed until the window closes, which emits exactly one final result per
 *       window.</li>
 * </ul>
 * All of them emit deterministically from record order and record timestamps alone, so the committed output of
 * a failure-free run and of a run that experienced unclean task handoffs must be identical if exactly-once
 * processing holds. Losing state or a committed offset shows up as missing or changed results; re-applying
 * already-committed input shows up as extra or changed results.
 *
 * <p>Commits happen only where the workload asks for them (at the end of every chunk, via a record header), so
 * every crash aborts all work of the current chunk on the crashed instance and the new owners have to restore
 * the committed state from the changelogs and reprocess the chunk. Progress is coordinated on observable
 * facts only: instance states, the group's committed input offsets and the log end offsets of the output topics.
 */
public final class EosDifferentialHarness {

    private static final Logger LOG = LoggerFactory.getLogger(EosDifferentialHarness.class);

    public static final String STATS_TOPIC = "stats";
    public static final String WINDOW_UPDATES_TOPIC = "window-updates";
    public static final String WINDOW_FINALS_TOPIC = "window-finals";

    public static final String STATS_STORE = "differential-stats-store";
    public static final String WINDOW_STORE = "differential-window-store";
    public static final String SUPPRESS_STORE = "differential-suppress-store";
    public static final String CHUNK_END_HEADER = "differential-chunk-end";

    /** Periodic commits are effectively disabled; commits are requested by the topology at chunk boundaries. */
    private static final long COMMIT_INTERVAL_MS = 60_000L;
    private static final int SESSION_TIMEOUT_MS = 10_000;
    private static final int HEARTBEAT_INTERVAL_MS = 1_000;
    private static final long PROGRESS_TIMEOUT_MS = 180_000L;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(60);

    private final EmbeddedKafkaCluster cluster;
    private final String groupProtocol;
    private final DifferentialWorkload workload;
    private final File stateRoot;

    public EosDifferentialHarness(final EmbeddedKafkaCluster cluster,
                                  final String groupProtocol,
                                  final DifferentialWorkload workload) {
        this.cluster = cluster;
        this.groupProtocol = groupProtocol;
        this.workload = workload;
        this.stateRoot = TestUtils.tempDirectory();
    }

    /** Runs the workload once on a single instance without any failures. */
    public RunResult runBaseline(final String applicationId) throws Exception {
        return run(applicationId, FailureSchedule.none(1));
    }

    /** Runs the workload chunk by chunk, applying the schedule's events between chunks. */
    public RunResult run(final String applicationId, final FailureSchedule schedule) throws Exception {
        final Topics topics = prepareTopics(applicationId);
        final RunContext context = new RunContext();
        final Map<String, Instance> instances = new LinkedHashMap<>();
        try {
            startInitialInstances(applicationId, schedule, topics, context, instances);
            processChunks(applicationId, schedule, topics, context, instances);
            closeCleanly(instances);
            failOnUnexpectedErrors(applicationId, context);
            return collectResults(topics, context, instances);
        } finally {
            for (final Instance instance : instances.values()) {
                if (!instance.closed) {
                    instance.streams.close(Duration.ofSeconds(30));
                }
            }
            Utils.delete(new File(stateRoot, applicationId));
        }
    }

    private Topics prepareTopics(final String applicationId) throws InterruptedException {
        final Topics topics = new Topics(applicationId);
        cluster.deleteAllTopics();
        // Retention is applied against record timestamps; never let the broker expire the harness topics.
        final Map<String, String> topicConfig = Collections.singletonMap(TopicConfig.RETENTION_MS_CONFIG, "-1");
        for (final String topic : List.of(topics.input, topics.stats, topics.windowUpdates, topics.windowFinals)) {
            cluster.createTopic(topic, workload.spec().partitions(), 1, topicConfig);
        }
        if (isStreamsProtocol()) {
            cluster.setGroupSessionTimeout(applicationId, SESSION_TIMEOUT_MS);
            cluster.setGroupHeartbeatTimeout(applicationId, HEARTBEAT_INTERVAL_MS);
        }
        return topics;
    }

    private void startInitialInstances(final String applicationId,
                                       final FailureSchedule schedule,
                                       final Topics topics,
                                       final RunContext context,
                                       final Map<String, Instance> instances) throws Exception {
        final List<KafkaStreams> initial = new ArrayList<>();
        for (final String name : schedule.initialInstanceNames()) {
            final Instance instance = createInstance(name, applicationId, topics, context);
            instances.put(name, instance);
            initial.add(instance.streams);
        }
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(initial, Duration.ofMillis(PROGRESS_TIMEOUT_MS));
        LOG.info("[{}] started {} with schedule '{}'", applicationId, schedule.initialInstanceNames(), schedule.describe());
    }

    private void processChunks(final String applicationId,
                               final FailureSchedule schedule,
                               final Topics topics,
                               final RunContext context,
                               final Map<String, Instance> instances) throws Exception {
        final Map<TopicPartition, Long> inputEndOffsets = new HashMap<>();
        for (int chunk = 0; chunk < workload.chunkCount(); chunk++) {
            final List<Instance> armed = new ArrayList<>();
            if (chunk > 0) {
                for (final FailureSchedule.Event event : schedule.eventsAfterChunk(chunk - 1)) {
                    applyEvent(event, applicationId, topics, context, instances, armed);
                }
            }

            produceChunk(topics.input, workload.chunk(chunk), inputEndOffsets);
            LOG.info("[{}] produced chunk {} ({} records); input end offsets {}",
                applicationId, chunk, workload.chunk(chunk).size(), inputEndOffsets);

            for (final Instance target : armed) {
                awaitCrash(applicationId, context, target);
            }
            waitForCommittedInputOffsets(applicationId, inputEndOffsets);
            failOnUnexpectedErrors(applicationId, context);
            LOG.info("[{}] chunk {} committed by {}", applicationId, chunk, liveInstanceNames(instances));
        }
    }

    private static void closeCleanly(final Map<String, Instance> instances) {
        for (final Instance instance : instances.values()) {
            if (!instance.closed) {
                if (!instance.streams.close(CLOSE_TIMEOUT)) {
                    throw new AssertionError("Instance " + instance.name + " did not close within " + CLOSE_TIMEOUT);
                }
                instance.closed = true;
            }
        }
    }

    private RunResult collectResults(final Topics topics,
                                     final RunContext context,
                                     final Map<String, Instance> instances) throws Exception {
        final Map<String, Map<String, List<String>>> committed = new LinkedHashMap<>();
        committed.put(STATS_TOPIC, readCommitted(topics.stats));
        committed.put(WINDOW_UPDATES_TOPIC, readCommitted(topics.windowUpdates));
        committed.put(WINDOW_FINALS_TOPIC, readCommitted(topics.windowFinals));

        final Map<String, Long> restoredByInstance = new TreeMap<>();
        for (final Instance instance : instances.values()) {
            restoredByInstance.put(instance.name, instance.restoreListener.totalNumRestored());
        }
        return new RunResult(
            committed,
            context.snapshotOwnership(),
            restoredByInstance,
            new TreeSet<>(context.crashesFired),
            context.processed.get()
        );
    }

    private void applyEvent(final FailureSchedule.Event event,
                            final String applicationId,
                            final Topics topics,
                            final RunContext context,
                            final Map<String, Instance> instances,
                            final List<Instance> armed) throws Exception {
        switch (event.kind) {
            case START: {
                final Instance instance = createInstance(event.instance, applicationId, topics, context);
                instances.put(event.instance, instance);
                instance.streams.start();
                waitForApplicationState(liveStreams(instances), State.RUNNING, Duration.ofMillis(PROGRESS_TIMEOUT_MS));
                LOG.info("[{}] started {}; live instances {}", applicationId, event.instance, liveInstanceNames(instances));
                break;
            }
            case CRASH: {
                final Instance target = instances.get(event.instance);
                if (target == null || target.closed) {
                    throw new IllegalStateException("Cannot crash " + event.instance + ": not running");
                }
                context.armCrash(event.instance, event.afterRecords);
                armed.add(target);
                LOG.info("[{}] armed crash on {} after {} records of the next chunk", applicationId, event.instance, event.afterRecords);
                break;
            }
            default:
                throw new IllegalStateException("Unknown event kind " + event.kind);
        }
    }

    private void awaitCrash(final String applicationId, final RunContext context, final Instance target) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> context.crashesFired.contains(target.name) && target.streams.state() == State.ERROR,
            PROGRESS_TIMEOUT_MS,
            () -> "Injected crash on " + target.name + " did not take effect: fired=" + context.crashesFired.contains(target.name)
                + " state=" + target.streams.state()
        );
        target.streams.close(CLOSE_TIMEOUT);
        target.closed = true;
        LOG.info("[{}] {} crashed and was shut down", applicationId, target.name);
    }

    private void failOnUnexpectedErrors(final String applicationId, final RunContext context) {
        if (!context.unexpectedErrors.isEmpty()) {
            throw new AssertionError("[" + applicationId + "] unexpected stream thread failures: " + context.unexpectedErrors);
        }
    }

    private static List<KafkaStreams> liveStreams(final Map<String, Instance> instances) {
        return instances.values().stream().filter(instance -> !instance.closed).map(instance -> instance.streams).collect(Collectors.toList());
    }

    private static List<String> liveInstanceNames(final Map<String, Instance> instances) {
        return instances.values().stream().filter(instance -> !instance.closed).map(instance -> instance.name).collect(Collectors.toList());
    }

    private boolean isStreamsProtocol() {
        return "streams".equalsIgnoreCase(groupProtocol);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Instances and topology
    // ---------------------------------------------------------------------------------------------------------

    private Instance createInstance(final String name,
                                    final String applicationId,
                                    final Topics topics,
                                    final RunContext context) {
        final File stateDir = new File(new File(stateRoot, applicationId), name + "-" + UUID.randomUUID());
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.CLIENT_ID_CONFIG, applicationId + "-" + name);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        config.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), (int) COMMIT_INTERVAL_MS);
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        // Treat every instance as caught up so that active tasks move to a freshly started instance right away.
        config.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, Long.MAX_VALUE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), SESSION_TIMEOUT_MS);
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), HEARTBEAT_INTERVAL_MS);

        final KafkaStreams streams = new KafkaStreams(buildTopology(name, topics, context), config);
        final TrackingStateRestoreListener restoreListener = new TrackingStateRestoreListener();
        streams.setGlobalStateRestoreListener(restoreListener);
        streams.setUncaughtExceptionHandler(exception -> {
            if (isInjectedCrash(exception)) {
                LOG.info("[{}] {} is shutting down because of the injected crash", applicationId, name);
            } else {
                LOG.error("[{}] {} failed unexpectedly", applicationId, name, exception);
                context.unexpectedErrors.add(name + ": " + exception);
            }
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        return new Instance(name, streams, restoreListener);
    }

    private Topology buildTopology(final String instance, final Topics topics, final RunContext context) {
        final DifferentialWorkload.Spec spec = workload.spec();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Long> input = builder.stream(topics.input, Consumed.with(Serdes.String(), Serdes.Long()));

        input
            .process(new StatsProcessorSupplier(instance, context))
            .to(topics.stats, Produced.with(Serdes.String(), Serdes.String()));

        final KTable<Windowed<String>, Long> windowSums = input
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(spec.windowSizeMs()), Duration.ofMillis(spec.graceMs())))
            .reduce(
                Long::sum,
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(WINDOW_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .withCachingDisabled()
            );

        windowSums
            .toStream()
            .map(EosDifferentialHarness::windowedKeyValue)
            .to(topics.windowUpdates, Produced.with(Serdes.String(), Serdes.String()));

        windowSums
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(SUPPRESS_STORE))
            .toStream()
            .map(EosDifferentialHarness::windowedKeyValue)
            .to(topics.windowFinals, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static KeyValue<String, String> windowedKeyValue(final Windowed<String> key, final Long sum) {
        return KeyValue.pair(key.key() + "@" + key.window().start(), String.valueOf(sum));
    }

    /** Thrown by the stats processor to simulate an unclean failure of a stream thread. */
    private static final class InjectedCrashException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        InjectedCrashException(final String message) {
            super(message);
        }
    }

    private static boolean isInjectedCrash(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof InjectedCrashException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class StatsProcessorSupplier implements ProcessorSupplier<String, Long, String, String> {
        private final String instance;
        private final RunContext context;

        StatsProcessorSupplier(final String instance, final RunContext context) {
            this.instance = instance;
            this.context = context;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATS_STORE), Serdes.String(), Serdes.String())
                    .withCachingDisabled()
            );
        }

        @Override
        public Processor<String, Long, String, String> get() {
            return new StatsProcessor(instance, context);
        }
    }

    /**
     * Keeps {@code count:sum:maxTimestamp:outOfOrderCount} per key, emits the new state after every record,
     * requests a commit at chunk ends and throws the injected crash when armed.
     */
    private static final class StatsProcessor implements Processor<String, Long, String, String> {
        private final String instance;
        private final RunContext runContext;
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> store;

        StatsProcessor(final String instance, final RunContext runContext) {
            this.instance = instance;
            this.runContext = runContext;
        }

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
            this.store = context.getStateStore(STATS_STORE);
            runContext.recordOwnership(context.taskId(), instance);
        }

        @Override
        public void process(final Record<String, Long> record) {
            final String updated = updateStats(store.get(record.key()), record.value(), record.timestamp());
            store.put(record.key(), updated);
            context.forward(record.withValue(updated));

            if (record.headers().lastHeader(CHUNK_END_HEADER) != null) {
                context.commit();
            }
            runContext.processed.incrementAndGet();
            if (runContext.shouldCrash(instance)) {
                throw new InjectedCrashException(instance + " crashed after processing " + record.key() + "@" + record.timestamp());
            }
        }

        static String updateStats(final String previous, final long value, final long timestamp) {
            long count = 0L;
            long sum = 0L;
            long maxTimestamp = Long.MIN_VALUE;
            long outOfOrder = 0L;
            if (previous != null) {
                final String[] parts = previous.split(":");
                count = Long.parseLong(parts[0]);
                sum = Long.parseLong(parts[1]);
                maxTimestamp = Long.parseLong(parts[2]);
                outOfOrder = Long.parseLong(parts[3]);
            }
            if (timestamp < maxTimestamp) {
                outOfOrder++;
            }
            return (count + 1) + ":" + (sum + value) + ":" + Math.max(maxTimestamp, timestamp) + ":" + outOfOrder;
        }
    }

    private static final class Instance {
        final String name;
        final KafkaStreams streams;
        final TrackingStateRestoreListener restoreListener;
        volatile boolean closed = false;

        Instance(final String name, final KafkaStreams streams, final TrackingStateRestoreListener restoreListener) {
            this.name = name;
            this.streams = streams;
            this.restoreListener = restoreListener;
        }
    }

    private static final class Topics {
        final String input;
        final String stats;
        final String windowUpdates;
        final String windowFinals;

        Topics(final String applicationId) {
            input = applicationId + "-input";
            stats = applicationId + "-" + STATS_TOPIC;
            windowUpdates = applicationId + "-" + WINDOW_UPDATES_TOPIC;
            windowFinals = applicationId + "-" + WINDOW_FINALS_TOPIC;
        }
    }

    /** Mutable state shared between the test thread and all instances of one run. */
    private static final class RunContext {
        final Map<TaskId, List<String>> ownership = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> crashCountdowns = new ConcurrentHashMap<>();
        final Set<String> crashesFired = ConcurrentHashMap.newKeySet();
        final List<String> unexpectedErrors = Collections.synchronizedList(new ArrayList<>());
        final AtomicLong processed = new AtomicLong();

        void recordOwnership(final TaskId taskId, final String instance) {
            ownership.compute(taskId, (id, owners) -> {
                final List<String> history = owners == null ? new ArrayList<>() : owners;
                if (history.isEmpty() || !history.get(history.size() - 1).equals(instance)) {
                    history.add(instance);
                }
                return history;
            });
        }

        Map<TaskId, List<String>> snapshotOwnership() {
            final Map<TaskId, List<String>> snapshot = new TreeMap<>();
            ownership.forEach((taskId, owners) -> snapshot.put(taskId, new ArrayList<>(owners)));
            return snapshot;
        }

        void armCrash(final String instance, final int afterRecords) {
            crashCountdowns.put(instance, new AtomicInteger(afterRecords));
        }

        boolean shouldCrash(final String instance) {
            final AtomicInteger countdown = crashCountdowns.get(instance);
            if (countdown != null && countdown.decrementAndGet() == 0) {
                crashesFired.add(instance);
                return true;
            }
            return false;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Producing input and observing progress
    // ---------------------------------------------------------------------------------------------------------

    private void produceChunk(final String topic,
                              final List<DifferentialWorkload.InputRecord> records,
                              final Map<TopicPartition, Long> endOffsets) throws Exception {
        final Properties producerConfig = TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, LongSerializer.class);
        try (final Producer<String, Long> producer = new KafkaProducer<>(producerConfig)) {
            final List<Future<RecordMetadata>> futures = new ArrayList<>(records.size());
            for (final DifferentialWorkload.InputRecord record : records) {
                final RecordHeaders headers = new RecordHeaders();
                if (record.chunkEnd) {
                    headers.add(CHUNK_END_HEADER, new byte[0]);
                }
                futures.add(producer.send(new ProducerRecord<>(topic, record.partition, record.timestamp, record.key, record.value, headers)));
            }
            for (final Future<RecordMetadata> future : futures) {
                final RecordMetadata metadata = future.get();
                endOffsets.merge(new TopicPartition(topic, metadata.partition()), metadata.offset() + 1, Math::max);
            }
        }
    }

    private void waitForCommittedInputOffsets(final String applicationId,
                                              final Map<TopicPartition, Long> expected) throws Exception {
        try (final Admin admin = cluster.createAdminClient()) {
            final Map<TopicPartition, Long> lastSeen = new HashMap<>();
            TestUtils.waitForCondition(
                () -> {
                    final Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(admin, applicationId);
                    lastSeen.clear();
                    committed.forEach((partition, offset) -> lastSeen.put(partition, offset == null ? null : offset.offset()));
                    for (final Map.Entry<TopicPartition, Long> entry : expected.entrySet()) {
                        final OffsetAndMetadata offset = committed.get(entry.getKey());
                        if (offset == null || offset.offset() < entry.getValue()) {
                            return false;
                        }
                    }
                    return true;
                },
                PROGRESS_TIMEOUT_MS,
                () -> "Application " + applicationId + " did not commit input offsets " + expected + "; last seen " + lastSeen
            );
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(final Admin admin, final String applicationId) {
        try {
            if (isStreamsProtocol()) {
                return admin
                    .listStreamsGroupOffsets(Collections.singletonMap(applicationId, new ListStreamsGroupOffsetsSpec()))
                    .partitionsToOffsetAndMetadata(applicationId)
                    .get();
            }
            return admin.listConsumerGroupOffsets(applicationId).partitionsToOffsetAndMetadata().get();
        } catch (final Exception e) {
            LOG.debug("Could not fetch committed offsets of {}", applicationId, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Reads every committed record of the topic, waiting first until no transaction is still open on it, and
     * returns the ordered {@code value@timestamp} sequence per key.
     */
    private Map<String, List<String>> readCommitted(final String topic) throws Exception {
        final String readerId = "differential-reader-" + UUID.randomUUID();
        final Properties committedConfig = TestUtils.consumerConfig(
            cluster.bootstrapServers(), readerId, StringDeserializer.class, StringDeserializer.class,
            mkProperties(mkMap(
                mkEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString()),
                mkEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            )));
        final Properties uncommittedConfig = TestUtils.consumerConfig(
            cluster.bootstrapServers(), readerId + "-hw", StringDeserializer.class, StringDeserializer.class,
            mkProperties(mkMap(
                mkEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString()),
                mkEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            )));

        try (final KafkaConsumer<String, String> committed = new KafkaConsumer<>(committedConfig);
             final KafkaConsumer<String, String> uncommitted = new KafkaConsumer<>(uncommittedConfig)) {
            final List<TopicPartition> partitions = committed.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .sorted((a, b) -> Integer.compare(a.partition(), b.partition()))
                .collect(Collectors.toList());
            committed.assign(partitions);
            uncommitted.assign(partitions);

            final Map<TopicPartition, Long> end = new HashMap<>();
            TestUtils.waitForCondition(
                () -> {
                    final Map<TopicPartition, Long> lastStable = committed.endOffsets(partitions);
                    final Map<TopicPartition, Long> highWatermark = uncommitted.endOffsets(partitions);
                    if (lastStable.equals(highWatermark)) {
                        end.clear();
                        end.putAll(highWatermark);
                        return true;
                    }
                    return false;
                },
                PROGRESS_TIMEOUT_MS,
                () -> "Open transactions remained on " + topic
            );

            committed.seekToBeginning(partitions);
            final Map<String, List<String>> byKey = new TreeMap<>();
            final long deadline = System.currentTimeMillis() + PROGRESS_TIMEOUT_MS;
            while (partitions.stream().anyMatch(partition -> committed.position(partition) < end.get(partition))) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("Timed out reading committed records of " + topic + " up to " + end);
                }
                final ConsumerRecords<String, String> records = committed.poll(Duration.ofMillis(500));
                for (final ConsumerRecord<String, String> record : records) {
                    byKey.computeIfAbsent(record.key(), key -> new ArrayList<>()).add(record.value() + "@" + record.timestamp());
                }
            }
            return byKey;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Results and comparison
    // ---------------------------------------------------------------------------------------------------------

    public static final class RunResult {
        private final Map<String, Map<String, List<String>>> committedByTopic;
        private final Map<TaskId, List<String>> ownershipHistory;
        private final Map<String, Long> restoredByInstance;
        private final Set<String> crashedInstances;
        private final long processedRecords;

        RunResult(final Map<String, Map<String, List<String>>> committedByTopic,
                  final Map<TaskId, List<String>> ownershipHistory,
                  final Map<String, Long> restoredByInstance,
                  final Set<String> crashedInstances,
                  final long processedRecords) {
            this.committedByTopic = committedByTopic;
            this.ownershipHistory = ownershipHistory;
            this.restoredByInstance = restoredByInstance;
            this.crashedInstances = crashedInstances;
            this.processedRecords = processedRecords;
        }

        public Set<String> topics() {
            return committedByTopic.keySet();
        }

        /** Ordered {@code value@timestamp} results per key for one of the logical output topics. */
        public Map<String, List<String>> committed(final String topic) {
            return committedByTopic.getOrDefault(topic, Collections.emptyMap());
        }

        public long committedRecordCount(final String topic) {
            return committed(topic).values().stream().mapToLong(List::size).sum();
        }

        public Map<TaskId, List<String>> ownershipHistory() {
            return ownershipHistory;
        }

        /** Number of tasks that were initialised on more than one distinct instance. */
        public int tasksWithMultipleOwners() {
            return (int) ownershipHistory.values().stream().filter(owners -> new TreeSet<>(owners).size() > 1).count();
        }

        /** Number of times any task moved from one instance to another. */
        public int ownershipChanges() {
            return ownershipHistory.values().stream().mapToInt(owners -> owners.size() - 1).sum();
        }

        public Map<String, Long> restoredByInstance() {
            return restoredByInstance;
        }

        public long restoredRecords() {
            return restoredByInstance.values().stream().mapToLong(Long::longValue).sum();
        }

        public Set<String> crashedInstances() {
            return crashedInstances;
        }

        /** Records processed by all instances, including work that was later aborted. */
        public long processedRecords() {
            return processedRecords;
        }

        public String describe() {
            final StringBuilder builder = new StringBuilder();
            builder.append("processed=").append(processedRecords)
                .append(" crashed=").append(crashedInstances)
                .append(" restoredByInstance=").append(restoredByInstance)
                .append(" committedRecords={");
            committedByTopic.forEach((topic, byKey) ->
                builder.append(topic).append('=').append(byKey.values().stream().mapToLong(List::size).sum()).append(' '));
            builder.append("} ownership={");
            ownershipHistory.forEach((taskId, owners) -> builder.append(taskId).append(": ").append(String.join(" -> ", owners)).append("; "));
            return builder.append('}').toString();
        }
    }

    public static final class Comparison {
        private final List<String> differences;

        Comparison(final List<String> differences) {
            this.differences = Collections.unmodifiableList(differences);
        }

        public boolean identical() {
            return differences.isEmpty();
        }

        public List<String> differences() {
            return differences;
        }

        public String report() {
            if (identical()) {
                return "Committed results are identical.";
            }
            return differences.size() + " key(s) with diverging committed results:\n  " + String.join("\n  ", differences);
        }
    }

    /** Compares the committed per-key result sequences of two runs, topic by topic. */
    public static Comparison compare(final RunResult baseline, final RunResult recovery) {
        final List<String> differences = new ArrayList<>();
        final Set<String> topics = new TreeSet<>(baseline.topics());
        topics.addAll(recovery.topics());
        for (final String topic : topics) {
            final Map<String, List<String>> baselineByKey = baseline.committed(topic);
            final Map<String, List<String>> recoveryByKey = recovery.committed(topic);
            final Set<String> keys = new TreeSet<>(baselineByKey.keySet());
            keys.addAll(recoveryByKey.keySet());
            for (final String key : keys) {
                final List<String> expected = baselineByKey.getOrDefault(key, Collections.emptyList());
                final List<String> actual = recoveryByKey.getOrDefault(key, Collections.emptyList());
                if (!expected.equals(actual)) {
                    differences.add(describeDifference(topic, key, expected, actual));
                }
            }
        }
        return new Comparison(differences);
    }

    private static String describeDifference(final String topic,
                                             final String key,
                                             final List<String> baseline,
                                             final List<String> recovery) {
        int divergence = 0;
        while (divergence < Math.min(baseline.size(), recovery.size()) && baseline.get(divergence).equals(recovery.get(divergence))) {
            divergence++;
        }
        final StringBuilder builder = new StringBuilder()
            .append('[').append(topic).append("] key=").append(key)
            .append(": baseline committed ").append(baseline.size())
            .append(" result(s), recovery committed ").append(recovery.size())
            .append(" result(s); first divergence at index ").append(divergence);
        if (divergence == baseline.size()) {
            builder.append(" -> recovery has ").append(recovery.size() - divergence).append(" extra result(s) ")
                .append(recovery.subList(divergence, Math.min(recovery.size(), divergence + 5)));
        } else if (divergence == recovery.size()) {
            builder.append(" -> recovery is missing ").append(baseline.size() - divergence).append(" result(s) ")
                .append(baseline.subList(divergence, Math.min(baseline.size(), divergence + 5)));
        } else {
            builder.append(" -> baseline ").append(baseline.subList(divergence, Math.min(baseline.size(), divergence + 3)))
                .append(" vs recovery ").append(recovery.subList(divergence, Math.min(recovery.size(), divergence + 3)));
        }
        return builder.toString();
    }
}
