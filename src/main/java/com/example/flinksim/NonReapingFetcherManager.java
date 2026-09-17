package com.example.flinksim;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.function.Supplier;

/**
 * A {@link SingleThreadFetcherManager} that keeps its idle fetcher alive, so one fetcher serves the
 * whole subtask and its id stays pinned at 0.
 *
 * <p>Without this, a reader holding one split at a time loses its fetcher in the gap between splits:
 * {@code SourceReaderBase} reaps idle fetchers whenever the element queue drains, and the next
 * {@code addSplits} builds a replacement with a fresh id. Every id is a permanent entry in the
 * handover queue's per-producer state, so the tally grows for as long as the job runs.
 *
 * <p>Suppressing the reap also suppresses termination, because {@code SourceReaderBase} only reports
 * end of input once no more splits are coming <em>and</em> every fetcher has shut down. {@link
 * #allowShutdown()} lifts the suppression and has to be called when the last split has been handed
 * out, or a bounded job would never finish.
 *
 * <p>This mirrors the workaround a connector needs while the queue retains producer state. It is
 * unnecessary against a queue that releases that state on fetcher shutdown.
 */
public final class NonReapingFetcherManager<E, SplitT extends SourceSplit>
        extends SingleThreadFetcherManager<E, SplitT> {

    private volatile boolean shutdownAllowed;

    public NonReapingFetcherManager(Supplier<SplitReader<E, SplitT>> splitReaderSupplier, Configuration config) {
        super(splitReaderSupplier, config);
    }

    /** Stops suppressing the reap, so the reader can observe end of input. There is no way back. */
    public void allowShutdown() {
        this.shutdownAllowed = true;
    }

    @Override
    public boolean maybeShutdownFinishedFetchers() {
        return this.shutdownAllowed && super.maybeShutdownFinishedFetchers();
    }
}
