package com.example.splitfetcherleaks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the simulated connector on a local {@link StreamExecutionEnvironment}.
 *
 * <p>Usage: {@code SimulationJob [key=value ...]} — see {@link SimulationConfig} for the options.
 */
public final class SimulationJob {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationJob.class);

    private SimulationJob() {}

    public static void main(String[] args) throws Exception {
        SimulationConfig config = SimulationConfig.fromArgs(args);
        LOG.info("Starting simulation with {}", config);

        Configuration flinkConfig = new Configuration();
        // Keep the handover queue between fetchers and the reader deliberately small so producers
        // actually block and have to be woken up.
        flinkConfig.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, config.queueCapacity());

        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(config.parallelism(), flinkConfig)) {

            if (config.checkpointIntervalMillis() > 0) {
                // The coordinator holds on to every assigned split until a checkpoint completes,
                // so a long run without checkpointing accumulates splits by design.
                env.enableCheckpointing(config.checkpointIntervalMillis());
            }

            DataStream<RandomRecord> records =
                    env.fromSource(
                                    new RandomEventSource(config),
                                    WatermarkStrategy.noWatermarks(),
                                    "random-event-source")
                            .setParallelism(config.parallelism())
                            .uid("random-event-source");

            records.map(new ProgressReporter(config))
                    .name("progress-reporter")
                    .uid("progress-reporter")
                    .setParallelism(config.parallelism())
                    .sinkTo(new DiscardingSink<>())
                    .name("discard")
                    .uid("discard");

            long startedAt = System.nanoTime();
            env.execute("flink-splitfetcher-leaks");
            long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

            LOG.info("Simulation finished in {} ms", elapsedMillis);
        }
    }

    /**
     * Counts records and periodically logs throughput, so a long soak run shows progress without
     * printing every record.
     */
    private static final class ProgressReporter extends RichMapFunction<RandomRecord, RandomRecord> {

        private static final long serialVersionUID = 1L;

        private final long reportIntervalMillis;

        private transient long recordCount;
        private transient long startedAt;
        private transient long lastReportAt;
        private transient long lastReportCount;

        ProgressReporter(SimulationConfig config) {
            this.reportIntervalMillis = config.reportIntervalMillis();
        }

        @Override
        public void open(OpenContext openContext) {
            startedAt = System.currentTimeMillis();
            lastReportAt = startedAt;
        }

        @Override
        public RandomRecord map(RandomRecord value) {
            recordCount++;
            if (reportIntervalMillis > 0) {
                long now = System.currentTimeMillis();
                if (now - lastReportAt >= reportIntervalMillis) {
                    long deltaRecords = recordCount - lastReportCount;
                    long deltaMillis = now - lastReportAt;
                    LOG.info(
                            "subtask {}: {} records total, {} rec/s over the last {} ms",
                            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                            recordCount,
                            deltaMillis == 0 ? deltaRecords : (deltaRecords * 1000L / deltaMillis),
                            deltaMillis);
                    lastReportAt = now;
                    lastReportCount = recordCount;
                }
            }
            return value;
        }

        @Override
        public void close() {
            LOG.info(
                    "subtask {} finished: {} records in {} ms",
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    recordCount,
                    System.currentTimeMillis() - startedAt);
        }
    }
}
