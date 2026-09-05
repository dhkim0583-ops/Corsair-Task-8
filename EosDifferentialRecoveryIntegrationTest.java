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
package org.apache.kafka.streams.integration;

import org.apache.kafka.streams.integration.differential.DifferentialWorkload;
import org.apache.kafka.streams.integration.differential.EosDifferentialHarness;
import org.apache.kafka.streams.integration.differential.EosDifferentialHarness.Comparison;
import org.apache.kafka.streams.integration.differential.EosDifferentialHarness.RunResult;
import org.apache.kafka.streams.integration.differential.FailureSchedule;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential exactly-once test: the same seeded, timestamped workload is processed by a stateful topology
 * once without interruption and once through unclean task handoffs (crashed instances plus a freshly started
 * instance, with changelog restoration on the new owners). The committed results of both runs must be identical.
 *
 * <p>On failure the assertion message contains the seed and the failure schedule. Replay a run by setting the
 * system properties {@value #SEED_PROPERTY} and {@value #SCHEDULE_PROPERTY}, or the environment variables
 * {@value #SEED_ENV} and {@value #SCHEDULE_ENV}.
 */
@Tag("integration")
@Timeout(900)
public class EosDifferentialRecoveryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(EosDifferentialRecoveryIntegrationTest.class);

    public static final String SEED_PROPERTY = "kafka.streams.eos.differential.seed";
    public static final String SCHEDULE_PROPERTY = "kafka.streams.eos.differential.schedule";
    public static final String SEED_ENV = "KAFKA_STREAMS_EOS_DIFFERENTIAL_SEED";
    public static final String SCHEDULE_ENV = "KAFKA_STREAMS_EOS_DIFFERENTIAL_SCHEDULE";

    private static final long DEFAULT_SEED = 20260904L;
    private static final int INITIAL_INSTANCES = 3;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    private String applicationId;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) {
        applicationId = "eos-differential-" + safeUniqueTestName(testInfo);
    }

    /**
     * Out-of-order records stay inside the grace period, so no record is ever dropped as late. Every
     * committed result of the recovery run must match the uninterrupted run exactly.
     */
    @ParameterizedTest
    @ValueSource(strings = {"classic", "streams"})
    public void shouldCommitIdenticalResultsAcrossUncleanTaskHandoffs(final String groupProtocol) throws Exception {
        runDifferentialComparison(groupProtocol, new DifferentialWorkload.Spec(seed()));
    }

    /**
     * Same as above, but the first record of every partition in every chunk after the first is late by more
     * than window size plus grace. An uninterrupted run drops these records; a run that recovered from an
     * unclean handoff must drop exactly the same records, otherwise closed windows receive additional
     * updates and already-emitted final results are emitted again.
     *
     * <p><b>This test currently fails and is kept as a focused reproducer.</b> The stats output (plain
     * key-value state) is identical in both runs, so offsets, state and output are committed atomically. The
     * windowed aggregation, however, decides whether a record is late using an in-memory "operator time" that is
     * not restored when a task is (re)initialised on another instance, even though the task's partition time is
     * committed alongside the offsets. The first record a new owner processes is therefore never late, so a
     * record that the uninterrupted run drops is aggregated into an already-closed window in the recovery run,
     * and {@code suppress(untilWindowCloses)} then emits a second "final" result for that window. The violated
     * property is failure transparency of exactly-once processing: the committed results after recovery are not
     * the results of processing the input exactly once, closed windows change after their final result was
     * committed, and a window's final result is published more than once. See KAFKA-9368 and KAFKA-20099.
     */
    @ParameterizedTest
    @ValueSource(strings = {"classic", "streams"})
    public void shouldCommitIdenticalResultsForLateRecordsAcrossUncleanTaskHandoffs(final String groupProtocol) throws Exception {
        runDifferentialComparison(groupProtocol, new DifferentialWorkload.Spec(seed()).withLateChunkLeaders(true));
    }

    private void runDifferentialComparison(final String groupProtocol, final DifferentialWorkload.Spec spec) throws Exception {
        final DifferentialWorkload workload = DifferentialWorkload.generate(spec);
        final FailureSchedule schedule = scheduleOverride()
            .map(FailureSchedule::parse)
            .orElseGet(() -> FailureSchedule.random(spec.seed(), INITIAL_INSTANCES, spec.chunks(), spec.recordsPerPartitionPerChunk()));
        final EosDifferentialHarness harness = new EosDifferentialHarness(CLUSTER, groupProtocol, workload);
        LOG.info("Differential run for {} with workload [{}] and schedule [{}]", applicationId, spec, schedule.describe());

        final RunResult baseline = harness.runBaseline(applicationId + "-baseline");
        LOG.info("Baseline run finished: {}", baseline.describe());
        final RunResult recovery = harness.run(applicationId + "-recovery", schedule);
        LOG.info("Recovery run finished: {}", recovery.describe());

        final String replay = replayInstructions(spec, schedule, baseline, recovery);

        // The baseline must have produced results for every key of every output, otherwise the comparison is vacuous.
        final Set<String> allKeys = new TreeSet<>();
        workload.keysByPartition().values().forEach(allKeys::addAll);
        assertEquals(allKeys, baseline.committed(EosDifferentialHarness.STATS_TOPIC).keySet(), "baseline stats output is incomplete\n" + replay);
        assertTrue(baseline.committedRecordCount(EosDifferentialHarness.WINDOW_UPDATES_TOPIC) > 0, "baseline emitted no window updates\n" + replay);
        assertTrue(baseline.committedRecordCount(EosDifferentialHarness.WINDOW_FINALS_TOPIC) > 0, "baseline emitted no final window results\n" + replay);

        // The recovery run must really have moved state between instances.
        assertEquals(schedule.crashCount(), recovery.crashedInstances().size(), "every scheduled crash must fire\n" + replay);
        assertTrue(recovery.tasksWithMultipleOwners() > 1,
            "expected more than one task to change owner, got " + recovery.tasksWithMultipleOwners() + "\n" + replay);
        assertTrue(recovery.ownershipChanges() >= schedule.crashCount() + schedule.startCount(),
            "expected at least one ownership change per schedule event, got " + recovery.ownershipChanges() + "\n" + replay);
        assertTrue(recovery.restoredRecords() > 0, "expected the new owners to restore state from the changelogs\n" + replay);

        final Comparison comparison = EosDifferentialHarness.compare(baseline, recovery);
        assertTrue(comparison.identical(),
            () -> "Committed results diverged between the uninterrupted run and the recovery run.\n" + comparison.report() + "\n" + replay);
    }

    private static long seed() {
        final String fromProperty = System.getProperty(SEED_PROPERTY);
        if (fromProperty != null && !fromProperty.isEmpty()) {
            return Long.parseLong(fromProperty.trim());
        }
        final String fromEnv = System.getenv(SEED_ENV);
        if (fromEnv != null && !fromEnv.isEmpty()) {
            return Long.parseLong(fromEnv.trim());
        }
        return DEFAULT_SEED;
    }

    private static Optional<String> scheduleOverride() {
        final String fromProperty = System.getProperty(SCHEDULE_PROPERTY);
        if (fromProperty != null && !fromProperty.trim().isEmpty()) {
            return Optional.of(fromProperty);
        }
        final String fromEnv = System.getenv(SCHEDULE_ENV);
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return Optional.of(fromEnv);
        }
        return Optional.empty();
    }

    private static String replayInstructions(final DifferentialWorkload.Spec spec,
                                             final FailureSchedule schedule,
                                             final RunResult baseline,
                                             final RunResult recovery) {
        return "Replay with: -D" + SEED_PROPERTY + "=" + spec.seed() + " -D" + SCHEDULE_PROPERTY + "=\"" + schedule.describe() + "\""
            + " (or " + SEED_ENV + "=" + spec.seed() + " " + SCHEDULE_ENV + "=\"" + schedule.describe() + "\")\n"
            + "Workload: " + spec + "\n"
            + "Failure schedule: " + schedule.describe() + "\n"
            + "Baseline run: " + baseline.describe() + "\n"
            + "Recovery run: " + recovery.describe();
    }
}
