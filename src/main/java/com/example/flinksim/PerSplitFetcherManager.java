package com.example.flinksim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Gives every split its own {@link SplitFetcher}, instead of multiplexing all splits onto one
 * long-lived fetcher the way {@code SingleThreadFetcherManager} does.
 *
 * <p>This is the assignment model of the connector being simulated, and it is the interesting one:
 * a fetcher lives only as long as the split it was created for, so a job that processes many short
 * splits churns through fetchers continuously. Fetcher ids come from a monotonic counter and are
 * never reused, so any per-fetcher bookkeeping kept downstream — in the handover queue, in the
 * metric groups — has to be released when the fetcher dies rather than merely abandoned.
 *
 * <p>Idle fetchers are reaped by the framework: once a split finishes, its fetcher has no assigned
 * splits left, and {@code SourceReaderBase} calls {@link #maybeShutdownFinishedFetchers()} which
 * shuts it down and drops it from the fetcher map.
 */
public final class PerSplitFetcherManager extends SplitFetcherManager<RandomRecord, EventSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PerSplitFetcherManager.class);

    public PerSplitFetcherManager(
            Supplier<SplitReader<RandomRecord, EventSplit>> splitReaderFactory, Configuration configuration) {
        super(splitReaderFactory, configuration);
    }

    @Override
    public void addSplits(List<EventSplit> splitsToAdd) {
        for (EventSplit split : splitsToAdd) {
            SplitFetcher<RandomRecord, EventSplit> fetcher = createSplitFetcher();
            fetcher.addSplits(Collections.singletonList(split));
            startFetcher(fetcher);
            LOG.debug("Started fetcher {} for split {}", fetcher.fetcherId(), split.splitId());
        }
    }

    @Override
    public void removeSplits(List<EventSplit> splitsToRemove) {
        // A fetcher owns exactly one split and goes away with it, so there is never a split to
        // detach from a fetcher that should outlive it.
        throw new UnsupportedOperationException(
                "Splits are not removed individually: each fetcher is discarded with its split.");
    }
}
