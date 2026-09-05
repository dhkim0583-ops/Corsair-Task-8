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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A replayable schedule of instance failures and (re)starts, expressed against workload chunk boundaries.
 *
 * <p>Every event is applied after the chunk it names has been fully committed and before the next chunk is
 * produced:
 * <ul>
 *   <li>{@code crash} arms an injected exception on one instance that fires after that instance has processed
 *       the given number of records of the following chunk. The exception kills the stream thread, so the tasks
 *       of that instance are closed dirty, their in-flight transaction is aborted and their local state is
 *       wiped. The instance is shut down and never rejoins under the same name.</li>
 *   <li>{@code start} starts a brand new instance with an empty state directory, which forces active tasks to
 *       move to it and to be restored from their changelogs.</li>
 * </ul>
 *
 * <p>The textual form produced by {@link #describe()} can be fed back through {@link #parse(String)} to replay a
 * failing run, for example {@code instances=3 0:crash:instance-1:7 1:start:instance-3 2:crash:instance-0:12}.
 */
public final class FailureSchedule {

    public enum Kind {
        CRASH,
        START
    }

    public static final class Event {
        public final int afterChunk;
        public final Kind kind;
        public final String instance;
        /** Number of records the target instance processes in the next chunk before the crash fires (CRASH only). */
        public final int afterRecords;

        Event(final int afterChunk, final Kind kind, final String instance, final int afterRecords) {
            if (afterChunk < 0) {
                throw new IllegalArgumentException("afterChunk must not be negative: " + afterChunk);
            }
            if (kind == Kind.CRASH && afterRecords < 1) {
                throw new IllegalArgumentException("A crash must fire after at least one record: " + afterRecords);
            }
            this.afterChunk = afterChunk;
            this.kind = kind;
            this.instance = instance;
            this.afterRecords = afterRecords;
        }

        public String describe() {
            final String base = afterChunk + ":" + kind.name().toLowerCase(Locale.ROOT) + ":" + instance;
            return kind == Kind.CRASH ? base + ":" + afterRecords : base;
        }

        @Override
        public String toString() {
            return describe();
        }
    }

    private final int initialInstances;
    private final List<Event> events;

    private FailureSchedule(final int initialInstances, final List<Event> events) {
        if (initialInstances < 1) {
            throw new IllegalArgumentException("At least one initial instance is required");
        }
        this.initialInstances = initialInstances;
        this.events = Collections.unmodifiableList(new ArrayList<>(events));
        validate();
    }

    public static String instanceName(final int index) {
        return "instance-" + index;
    }

    /** A schedule without any failures, used for the uninterrupted baseline run. */
    public static FailureSchedule none(final int instances) {
        return new FailureSchedule(instances, Collections.emptyList());
    }

    /**
     * Derives a schedule from the seed: alternating crashes and fresh starts after every chunk but the last one,
     * beginning with a crash. Crash targets and crash delays are drawn from the seeded random generator. The crash
     * delay never exceeds half the per-partition chunk size, so a crash is guaranteed to fire as long as the target
     * owns at least one input partition.
     */
    public static FailureSchedule random(final long seed,
                                         final int initialInstances,
                                         final int chunks,
                                         final int recordsPerPartitionPerChunk) {
        final Random random = new Random(seed * 31L + 17L);
        final List<String> live = new ArrayList<>();
        for (int i = 0; i < initialInstances; i++) {
            live.add(instanceName(i));
        }
        int nextInstance = initialInstances;
        final List<Event> events = new ArrayList<>();
        for (int chunk = 0; chunk < chunks - 1; chunk++) {
            final boolean crash = chunk % 2 == 0 && live.size() > 1;
            if (crash) {
                final String target = live.remove(random.nextInt(live.size()));
                final int afterRecords = 1 + random.nextInt(Math.max(1, recordsPerPartitionPerChunk / 2));
                events.add(new Event(chunk, Kind.CRASH, target, afterRecords));
            } else {
                final String name = instanceName(nextInstance++);
                live.add(name);
                events.add(new Event(chunk, Kind.START, name, 0));
            }
        }
        return new FailureSchedule(initialInstances, events);
    }

    public static FailureSchedule parse(final String text) {
        final String[] tokens = text.trim().split("\\s+");
        if (tokens.length == 0 || !tokens[0].startsWith("instances=")) {
            throw new IllegalArgumentException("Schedule must start with 'instances=<n>': " + text);
        }
        final int initialInstances = Integer.parseInt(tokens[0].substring("instances=".length()));
        final List<Event> events = new ArrayList<>();
        for (int i = 1; i < tokens.length; i++) {
            final String[] parts = tokens[i].split(":");
            if (parts.length < 3) {
                throw new IllegalArgumentException("Malformed schedule event '" + tokens[i] + "' in: " + text);
            }
            final int afterChunk = Integer.parseInt(parts[0]);
            final Kind kind = Kind.valueOf(parts[1].toUpperCase(Locale.ROOT));
            final int afterRecords = kind == Kind.CRASH ? Integer.parseInt(parts[3]) : 0;
            events.add(new Event(afterChunk, kind, parts[2], afterRecords));
        }
        return new FailureSchedule(initialInstances, events);
    }

    private void validate() {
        final Set<String> live = new LinkedHashSet<>(initialInstanceNames());
        int lastChunk = -1;
        for (final Event event : events) {
            if (event.afterChunk < lastChunk) {
                throw new IllegalArgumentException("Events must be ordered by chunk: " + describe());
            }
            lastChunk = event.afterChunk;
            switch (event.kind) {
                case CRASH:
                    if (!live.remove(event.instance)) {
                        throw new IllegalArgumentException(
                            "Cannot crash '" + event.instance + "', it is not running at that point: " + describe());
                    }
                    if (live.isEmpty()) {
                        throw new IllegalArgumentException("A crash must leave at least one instance running: " + describe());
                    }
                    break;
                case START:
                    if (!live.add(event.instance)) {
                        throw new IllegalArgumentException(
                            "Cannot start '" + event.instance + "', an instance with that name is running: " + describe());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown event kind " + event.kind);
            }
        }
    }

    public int initialInstances() {
        return initialInstances;
    }

    public List<String> initialInstanceNames() {
        final List<String> names = new ArrayList<>(initialInstances);
        for (int i = 0; i < initialInstances; i++) {
            names.add(instanceName(i));
        }
        return names;
    }

    public List<Event> events() {
        return events;
    }

    public List<Event> eventsAfterChunk(final int chunk) {
        return events.stream().filter(event -> event.afterChunk == chunk).collect(Collectors.toList());
    }

    public int crashCount() {
        return (int) events.stream().filter(event -> event.kind == Kind.CRASH).count();
    }

    public int startCount() {
        return (int) events.stream().filter(event -> event.kind == Kind.START).count();
    }

    /** Textual form accepted by {@link #parse(String)}. */
    public String describe() {
        final StringBuilder builder = new StringBuilder("instances=").append(initialInstances);
        for (final Event event : events) {
            builder.append(' ').append(event.describe());
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return describe();
    }
}
