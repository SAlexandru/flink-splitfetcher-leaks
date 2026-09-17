package com.example.flinksim;

import java.io.Serializable;

/**
 * Knobs for the simulation.
 *
 * @param maxEvents how many events the generator produces before signalling no-more-splits; a
 *     negative value means run forever (soak mode)
 * @param recordsPerEvent records synthesized per event, i.e. how long a split lives
 * @param payloadBytes size of each record's random payload
 * @param batchSize records returned per {@code fetch()} call; small values mean more trips through
 *     the handover queue
 * @param queueCapacity {@code source.element-queue-capacity}; small values force producers to block
 *     and be woken up, which is what exercises the handover queue's wakeup path
 * @param parallelism source parallelism
 * @param fetcherMode how fetchers are allocated to splits; see {@link FetcherMode}. Defaults to
 *     {@code SHARED}, Flink's stock behaviour and what a connector gets if it does nothing special
 * @param reportIntervalMillis how often to log progress, 0 to disable
 * @param checkpointIntervalMillis checkpoint interval, 0 to disable. Worth leaving on for long
 *     runs: the coordinator holds every assigned split in {@code SplitAssignmentTracker} until a
 *     checkpoint completes, so with checkpointing off the split objects pile up by design and
 *     obscure anything else that is accumulating.
 */
public record SimulationConfig(
        long maxEvents,
        int recordsPerEvent,
        int payloadBytes,
        int batchSize,
        int queueCapacity,
        int parallelism,
        FetcherMode fetcherMode,
        long reportIntervalMillis,
        long checkpointIntervalMillis)
        implements Serializable {

    public static SimulationConfig defaults() {
        return new SimulationConfig(2_000L, 50, 256, 8, 2, 1, FetcherMode.SHARED, 5_000L, 10_000L);
    }

    public boolean bounded() {
        return maxEvents >= 0;
    }

    /**
     * Parses {@code key=value} command line arguments, falling back to {@link #defaults()} for
     * anything not supplied.
     */
    public static SimulationConfig fromArgs(String[] args) {
        SimulationConfig config = defaults();
        long maxEvents = config.maxEvents();
        int recordsPerEvent = config.recordsPerEvent();
        int payloadBytes = config.payloadBytes();
        int batchSize = config.batchSize();
        int queueCapacity = config.queueCapacity();
        int parallelism = config.parallelism();
        FetcherMode fetcherMode = config.fetcherMode();
        long reportIntervalMillis = config.reportIntervalMillis();
        long checkpointIntervalMillis = config.checkpointIntervalMillis();

        for (String arg : args) {
            int split = arg.indexOf('=');
            if (split < 0) {
                throw new IllegalArgumentException("Expected key=value, got: " + arg);
            }
            String key = arg.substring(0, split);
            String value = arg.substring(split + 1);
            switch (key) {
                case "maxEvents" -> maxEvents = Long.parseLong(value);
                case "recordsPerEvent" -> recordsPerEvent = Integer.parseInt(value);
                case "payloadBytes" -> payloadBytes = Integer.parseInt(value);
                case "batchSize" -> batchSize = Integer.parseInt(value);
                case "queueCapacity" -> queueCapacity = Integer.parseInt(value);
                case "parallelism" -> parallelism = Integer.parseInt(value);
                case "fetcherMode" -> fetcherMode =
                        FetcherMode.valueOf(value.toUpperCase(java.util.Locale.ROOT));
                case "reportIntervalMillis" -> reportIntervalMillis = Long.parseLong(value);
                case "checkpointIntervalMillis" -> checkpointIntervalMillis = Long.parseLong(value);
                default -> throw new IllegalArgumentException("Unknown option: " + key);
            }
        }

        return new SimulationConfig(
                maxEvents,
                recordsPerEvent,
                payloadBytes,
                batchSize,
                queueCapacity,
                parallelism,
                fetcherMode,
                reportIntervalMillis,
                checkpointIntervalMillis);
    }
}
