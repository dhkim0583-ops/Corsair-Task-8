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

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * A bounded, timestamped input workload that is fully determined by a seed.
 *
 * <p>The workload is organised in <em>chunks</em>. Every chunk contains the same number of records for every
 * input partition, which lets a failure schedule reason about progress in terms of "records processed by an
 * instance during a chunk" without knowing which partitions that instance owns. The last record of every
 * partition in every chunk is flagged as a chunk end; the harness turns that flag into a header so the
 * topology can request a commit exactly at chunk boundaries.
 *
 * <p>Record timestamps advance with a bounded random step per partition and are perturbed with bounded
 * out-of-order jitter that always stays inside the grace period. When {@link Spec#withLateChunkLeaders(boolean)}
 * is enabled, the first record of every partition in every chunk after the first one is instead placed more
 * than a full window plus the grace period behind the stream time observed so far, i.e. it is a record that an
 * uninterrupted run must drop as late.
 *
 * <p>The final chunk is followed by one "flush" record per key whose timestamp is far enough ahead to close
 * every previously opened window, so that suppressed final results are emitted before the run ends.
 *
 * <p>All timestamps are relative to a base timestamp. By default the base is the current wall clock aligned down
 * to a window boundary: brokers apply time-based retention to the topics (and to the changelog topics that Kafka
 * Streams creates) using record timestamps, so a workload that is dated far in the past would be deleted while
 * the test is still running. Everything that matters for the comparison, i.e. the relative timestamps, window
 * boundaries, out-of-order and late records, is fully determined by the seed.
 */
public final class DifferentialWorkload {

    /** Marker for {@link Spec#withBaseTimestampMs(long)}: align the workload to the current wall clock. */
    public static final long ALIGN_BASE_TIMESTAMP_TO_NOW = -1L;

    /** Percentage of records whose timestamp is perturbed backwards (bounded by {@link Spec#maxDisorderMs}). */
    private static final int DISORDER_PERCENT = 30;

    public static final class Spec {
        private final long seed;
        private int partitions = 4;
        private int keysPerPartition = 3;
        private int chunks = 4;
        private int recordsPerPartitionPerChunk = 40;
        private long windowSizeMs = 10_000L;
        private long graceMs = 5_000L;
        private long maxTimestampStepMs = 800L;
        private long maxDisorderMs = 3_000L;
        private boolean lateChunkLeaders = false;
        private long baseTimestampMs = ALIGN_BASE_TIMESTAMP_TO_NOW;

        public Spec(final long seed) {
            this.seed = seed;
        }

        public Spec withPartitions(final int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Spec withKeysPerPartition(final int keysPerPartition) {
            this.keysPerPartition = keysPerPartition;
            return this;
        }

        public Spec withChunks(final int chunks) {
            this.chunks = chunks;
            return this;
        }

        public Spec withRecordsPerPartitionPerChunk(final int recordsPerPartitionPerChunk) {
            this.recordsPerPartitionPerChunk = recordsPerPartitionPerChunk;
            return this;
        }

        public Spec withWindow(final long windowSizeMs, final long graceMs) {
            this.windowSizeMs = windowSizeMs;
            this.graceMs = graceMs;
            return this;
        }

        public Spec withMaxTimestampStepMs(final long maxTimestampStepMs) {
            this.maxTimestampStepMs = maxTimestampStepMs;
            return this;
        }

        /**
         * Maximum backwards jitter applied to a record timestamp. Must be smaller than the grace period,
         * otherwise ordinary records may be dropped as late.
         */
        public Spec withMaxDisorderMs(final long maxDisorderMs) {
            this.maxDisorderMs = maxDisorderMs;
            return this;
        }

        public Spec withLateChunkLeaders(final boolean lateChunkLeaders) {
            this.lateChunkLeaders = lateChunkLeaders;
            return this;
        }

        /**
         * Fixes the base timestamp instead of aligning it to the current wall clock. Only useful for replaying a
         * run within the broker's retention window, since retention is applied against record timestamps.
         */
        public Spec withBaseTimestampMs(final long baseTimestampMs) {
            this.baseTimestampMs = baseTimestampMs;
            return this;
        }

        public long seed() {
            return seed;
        }

        public int partitions() {
            return partitions;
        }

        public int keysPerPartition() {
            return keysPerPartition;
        }

        public int chunks() {
            return chunks;
        }

        public int recordsPerPartitionPerChunk() {
            return recordsPerPartitionPerChunk;
        }

        public long windowSizeMs() {
            return windowSizeMs;
        }

        public long graceMs() {
            return graceMs;
        }

        public boolean lateChunkLeaders() {
            return lateChunkLeaders;
        }

        public long baseTimestampMs() {
            return baseTimestampMs;
        }

        private void validate() {
            if (partitions < 1 || keysPerPartition < 1 || chunks < 1 || recordsPerPartitionPerChunk < 1) {
                throw new IllegalArgumentException("Workload dimensions must be positive: " + this);
            }
            if (maxDisorderMs >= graceMs) {
                throw new IllegalArgumentException(
                    "maxDisorderMs must stay below the grace period so that ordinary records are never late: " + this);
            }
            if (windowSizeMs <= 0 || maxTimestampStepMs < 0) {
                throw new IllegalArgumentException("windowSizeMs must be positive and maxTimestampStepMs non-negative: " + this);
            }
        }

        @Override
        public String toString() {
            return "seed=" + seed
                + " partitions=" + partitions
                + " keysPerPartition=" + keysPerPartition
                + " chunks=" + chunks
                + " recordsPerPartitionPerChunk=" + recordsPerPartitionPerChunk
                + " windowSizeMs=" + windowSizeMs
                + " graceMs=" + graceMs
                + " maxTimestampStepMs=" + maxTimestampStepMs
                + " maxDisorderMs=" + maxDisorderMs
                + " lateChunkLeaders=" + lateChunkLeaders
                + " baseTimestampMs=" + (baseTimestampMs == ALIGN_BASE_TIMESTAMP_TO_NOW ? "now" : String.valueOf(baseTimestampMs));
        }
    }

    public static final class InputRecord {
        public final int chunk;
        public final int partition;
        public final String key;
        public final long value;
        public final long timestamp;
        public final boolean chunkEnd;

        InputRecord(final int chunk,
                    final int partition,
                    final String key,
                    final long value,
                    final long timestamp,
                    final boolean chunkEnd) {
            this.chunk = chunk;
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.chunkEnd = chunkEnd;
        }

        @Override
        public String toString() {
            return "chunk=" + chunk + " partition=" + partition + " key=" + key
                + " value=" + value + " timestamp=" + timestamp + (chunkEnd ? " chunkEnd" : "");
        }
    }

    private final Spec spec;
    private final long baseTimestampMs;
    private final List<List<InputRecord>> chunks;
    private final Map<Integer, List<String>> keysByPartition;

    private DifferentialWorkload(final Spec spec,
                                 final long baseTimestampMs,
                                 final List<List<InputRecord>> chunks,
                                 final Map<Integer, List<String>> keysByPartition) {
        this.spec = spec;
        this.baseTimestampMs = baseTimestampMs;
        this.chunks = chunks;
        this.keysByPartition = keysByPartition;
    }

    public static DifferentialWorkload generate(final Spec spec) {
        spec.validate();
        final Random random = new Random(spec.seed);
        final Map<Integer, List<String>> keysByPartition = assignKeys(spec);
        final long baseTimestampMs = spec.baseTimestampMs == ALIGN_BASE_TIMESTAMP_TO_NOW
            ? System.currentTimeMillis() / spec.windowSizeMs * spec.windowSizeMs
            : spec.baseTimestampMs;

        final long[] clock = new long[spec.partitions];
        final long[] maxObservedTimestamp = new long[spec.partitions];
        for (int partition = 0; partition < spec.partitions; partition++) {
            clock[partition] = baseTimestampMs;
            maxObservedTimestamp[partition] = baseTimestampMs;
        }

        final List<List<InputRecord>> chunks = new ArrayList<>(spec.chunks);
        long globalMaxTimestamp = baseTimestampMs;
        for (int chunk = 0; chunk < spec.chunks; chunk++) {
            final List<InputRecord> records = new ArrayList<>(spec.partitions * spec.recordsPerPartitionPerChunk);
            final boolean lastChunk = chunk == spec.chunks - 1;
            for (int partition = 0; partition < spec.partitions; partition++) {
                final List<String> keys = keysByPartition.get(partition);
                for (int i = 0; i < spec.recordsPerPartitionPerChunk; i++) {
                    clock[partition] += random.nextInt((int) spec.maxTimestampStepMs + 1);
                    final long timestamp;
                    if (spec.lateChunkLeaders && chunk > 0 && i == 0) {
                        // Strictly later than window size + grace behind the stream time observed so far:
                        // an uninterrupted run is guaranteed to drop this record for every window it matches.
                        timestamp = maxObservedTimestamp[partition] - spec.graceMs - spec.windowSizeMs - 1;
                    } else if (random.nextInt(100) < DISORDER_PERCENT) {
                        timestamp = clock[partition] - random.nextInt((int) spec.maxDisorderMs + 1);
                    } else {
                        timestamp = clock[partition];
                    }
                    final String key = keys.get(random.nextInt(keys.size()));
                    final long value = 1 + random.nextInt(100);
                    maxObservedTimestamp[partition] = Math.max(maxObservedTimestamp[partition], timestamp);
                    globalMaxTimestamp = Math.max(globalMaxTimestamp, timestamp);
                    final boolean chunkEnd = !lastChunk && i == spec.recordsPerPartitionPerChunk - 1;
                    records.add(new InputRecord(chunk, partition, key, value, timestamp, chunkEnd));
                }
            }
            if (lastChunk) {
                final long flushTimestamp = globalMaxTimestamp + spec.windowSizeMs + spec.graceMs + 1;
                for (int partition = 0; partition < spec.partitions; partition++) {
                    final List<String> keys = keysByPartition.get(partition);
                    for (int k = 0; k < keys.size(); k++) {
                        final boolean chunkEnd = k == keys.size() - 1;
                        records.add(new InputRecord(chunk, partition, keys.get(k), 0L, flushTimestamp, chunkEnd));
                    }
                }
            }
            chunks.add(Collections.unmodifiableList(records));
        }
        return new DifferentialWorkload(spec, baseTimestampMs, Collections.unmodifiableList(chunks), keysByPartition);
    }

    /**
     * Picks {@code keysPerPartition} keys for every partition using the producer's default
     * (murmur2) key partitioning, so that the generated key set is stable across runs.
     */
    private static Map<Integer, List<String>> assignKeys(final Spec spec) {
        final Map<Integer, List<String>> keysByPartition = new TreeMap<>();
        for (int partition = 0; partition < spec.partitions; partition++) {
            keysByPartition.put(partition, new ArrayList<>(spec.keysPerPartition));
        }
        int assigned = 0;
        try (final StringSerializer serializer = new StringSerializer()) {
            for (int n = 0; assigned < spec.partitions * spec.keysPerPartition; n++) {
                final String key = String.format("key-%03d", n);
                final int partition = BuiltInPartitioner.partitionForKey(serializer.serialize(null, key), spec.partitions);
                final List<String> keys = keysByPartition.get(partition);
                if (keys.size() < spec.keysPerPartition) {
                    keys.add(key);
                    assigned++;
                }
            }
        }
        return Collections.unmodifiableMap(keysByPartition);
    }

    public Spec spec() {
        return spec;
    }

    /** The base timestamp all record timestamps of this workload are relative to. */
    public long baseTimestampMs() {
        return baseTimestampMs;
    }

    public int chunkCount() {
        return chunks.size();
    }

    public List<InputRecord> chunk(final int index) {
        return chunks.get(index);
    }

    public List<InputRecord> allRecords() {
        final List<InputRecord> all = new ArrayList<>();
        chunks.forEach(all::addAll);
        return all;
    }

    public Map<Integer, List<String>> keysByPartition() {
        return keysByPartition;
    }

    public int recordCount() {
        return chunks.stream().mapToInt(List::size).sum();
    }
}
