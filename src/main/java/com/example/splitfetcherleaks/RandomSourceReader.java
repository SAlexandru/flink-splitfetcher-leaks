package com.example.splitfetcherleaks;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Source reader wired on top of {@link SourceReaderBase}.
 *
 * <p>{@code SourceReaderBase} is taken directly rather than {@code
 * SingleThreadMultiplexSourceReaderBase} because the latter only accepts a {@link
 * SingleThreadFetcherManager}, which would multiplex every split onto one fetcher and hide the
 * fetcher churn this simulation is about. Which manager is used is left configurable so the two
 * models can be compared in the same job.
 */
public final class RandomSourceReader
        extends SourceReaderBase<RandomRecord, RandomRecord, EventSplit, EventSplitState> {

    private final SourceReaderContext readerContext;

    public RandomSourceReader(SimulationConfig simulationConfig, Configuration config, SourceReaderContext context) {
        super(fetcherManager(simulationConfig, config), recordEmitter(), config, context);
        this.readerContext = context;
    }

    private static SplitFetcherManager<RandomRecord, EventSplit> fetcherManager(
            SimulationConfig simulationConfig, Configuration config) {
        Supplier<SplitReader<RandomRecord, EventSplit>> readerFactory =
                () -> new RandomDataSplitReader(simulationConfig);
        return switch (simulationConfig.fetcherMode()) {
            case SHARED -> new SingleThreadFetcherManager<>(readerFactory, config);
            case NON_REAPING -> new NonReapingFetcherManager<>(readerFactory, config);
            case PER_SPLIT -> new PerSplitFetcherManager(readerFactory, config);
        };
    }

    @Override
    public void notifyNoMoreSplits() {
        // A non-reaping manager never reports that its fetchers have shut down, and the base reader
        // needs that to reach end of input. Once no more splits are coming, let it reap.
        if (splitFetcherManager instanceof NonReapingFetcherManager<?, ?> nonReaping) {
            nonReaping.allowShutdown();
        }
        super.notifyNoMoreSplits();
    }

    /** Records need no conversion; the split state just advances so checkpoints can resume. */
    private static RecordEmitter<RandomRecord, RandomRecord, EventSplitState> recordEmitter() {
        return (element, output, splitState) -> {
            output.collect(element);
            splitState.setEmittedCount(element.sequence() + 1);
        };
    }

    @Override
    public void start() {
        // Ask for the first split; from then on each finished split triggers the next request.
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            readerContext.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, EventSplitState> finishedSplitIds) {
        // One split at a time: only ask for the next one once the current one is done.
        readerContext.sendSplitRequest();
    }

    @Override
    protected EventSplitState initializedState(EventSplit split) {
        return new EventSplitState(split);
    }

    @Override
    protected EventSplit toSplitType(String splitId, EventSplitState splitState) {
        return splitState.toEventSplit();
    }
}
