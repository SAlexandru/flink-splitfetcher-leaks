package com.example.flinksim;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A split-based source whose work items come from a random generator rather than from remote
 * storage.
 *
 * <p>The moving parts mirror an object-store connector driven by a notification topic:
 *
 * <ul>
 *   <li>{@link EventGeneratorEnumerator} plays the role of the topic consumer, turning each event
 *       into one indivisible split and assigning them one at a time;
 *   <li>{@link PerSplitFetcherManager} gives each split its own fetcher, as that connector does;
 *   <li>{@link RandomDataSplitReader} stands in for the object reader, synthesizing records instead
 *       of decoding a stream.
 * </ul>
 */
public final class RandomEventSource implements Source<RandomRecord, EventSplit, EnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final SimulationConfig config;

    public RandomEventSource(SimulationConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.bounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RandomRecord, EventSplit> createReader(SourceReaderContext readerContext) {
        Configuration readerConfig = new Configuration();
        readerConfig.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, config.queueCapacity());
        return new RandomSourceReader(config, readerConfig, readerContext);
    }

    @Override
    public SplitEnumerator<EventSplit, EnumeratorState> createEnumerator(
            SplitEnumeratorContext<EventSplit> enumContext) {
        return new EventGeneratorEnumerator(enumContext, config, null);
    }

    @Override
    public SplitEnumerator<EventSplit, EnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<EventSplit> enumContext, EnumeratorState checkpoint) {
        return new EventGeneratorEnumerator(enumContext, config, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<EventSplit> getSplitSerializer() {
        return EventSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
        return EnumeratorStateSerializer.INSTANCE;
    }
}
