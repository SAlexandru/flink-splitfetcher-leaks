package com.example.splitfetcherleaks;

import java.io.Serializable;
import java.util.List;

/**
 * Checkpointed enumerator state: how many events the generator has already produced, plus any
 * splits that were handed out but given back by a failing reader.
 *
 * @param generatedEvents number of events emitted so far, so a restored generator does not repeat ids
 * @param pendingSplits splits returned via {@code addSplitsBack} that still need an owner
 */
public record EnumeratorState(long generatedEvents, List<EventSplit> pendingSplits) implements Serializable {}
