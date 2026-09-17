package com.example.splitfetcherleaks;

/** How the reader allocates {@code SplitFetcher}s to splits. */
public enum FetcherMode {

    /**
     * Flink's stock {@code SingleThreadFetcherManager}: one fetcher, reused for whatever splits are
     * assigned to it.
     *
     * <p>The catch is that "reused" depends on the fetcher still being alive when the next split
     * turns up. {@code SourceReaderBase} reaps idle fetchers every time the element queue drains,
     * and a reader that holds one split at a time is idle in the gap between finishing a split and
     * being handed the next. So in this workload the shared fetcher is torn down and rebuilt on
     * every split, and each rebuild takes a fresh id.
     */
    SHARED,

    /**
     * The stock manager with idle reaping suppressed, so one fetcher really does last the lifetime
     * of the subtask and its id stays pinned at 0.
     *
     * <p>This is the shape of the workaround a connector has to adopt to avoid the id churn while
     * the handover queue still holds per-producer state forever. It costs one parked thread per
     * subtask, and reaping has to be re-enabled when no more splits are coming or the reader can
     * never reach end of input.
     */
    NON_REAPING,

    /** A fetcher per split. The upper bound on churn, useful for making the effect obvious fast. */
    PER_SPLIT
}
