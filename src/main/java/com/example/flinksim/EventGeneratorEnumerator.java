package com.example.flinksim;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * Produces random events and hands them to readers one split at a time.
 *
 * <p>This stands in for an enumerator driven by a notification topic: instead of polling a broker
 * for messages that point at objects in remote storage, it generates the descriptors itself. What
 * matters for the simulation is the assignment discipline, which is kept identical — every event
 * becomes one indivisible split, and a reader only ever holds a single split, asking for the next
 * one after it finishes the current one.
 *
 * <p>All methods here run on the coordinator thread, so the mutable state needs no synchronization.
 */
public final class EventGeneratorEnumerator implements SplitEnumerator<EventSplit, EnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(EventGeneratorEnumerator.class);

    private final SplitEnumeratorContext<EventSplit> context;
    private final SimulationConfig config;
    private final Random random;

    /** Splits handed back by a failed reader. Drained before new events are generated. */
    private final Queue<EventSplit> pendingSplits = new ArrayDeque<>();

    /** Readers that asked for a split and have not been served yet. */
    private final Queue<Integer> pendingRequests = new ArrayDeque<>();

    private long generatedEvents;

    public EventGeneratorEnumerator(
            SplitEnumeratorContext<EventSplit> context, SimulationConfig config, EnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.random = new Random();
        if (restoredState != null) {
            this.generatedEvents = restoredState.generatedEvents();
            this.pendingSplits.addAll(restoredState.pendingSplits());
        }
    }

    @Override
    public void start() {
        LOG.info(
                "Event generator started: maxEvents={}, recordsPerEvent={}",
                config.bounded() ? config.maxEvents() : "unbounded",
                config.recordsPerEvent());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        pendingRequests.add(subtaskId);
        serveRequests();
    }

    @Override
    public void addSplitsBack(List<EventSplit> splits, int subtaskId) {
        LOG.info("Reader {} gave back {} split(s) after a failure", subtaskId, splits.size());
        pendingSplits.addAll(splits);
        serveRequests();
    }

    @Override
    public void addReader(int subtaskId) {
        // Readers drive the handshake by sending a split request once they are ready, so there is
        // nothing to push here.
    }

    /** Gives one split to each waiting reader, generating fresh events as needed. */
    private void serveRequests() {
        while (!pendingRequests.isEmpty()) {
            EventSplit split = nextSplit();
            if (split == null) {
                // Generator is exhausted: tell everyone still waiting that nothing more is coming.
                while (!pendingRequests.isEmpty()) {
                    context.signalNoMoreSplits(pendingRequests.poll());
                }
                return;
            }
            context.assignSplit(split, pendingRequests.poll());
        }
    }

    /** A recycled split if one is waiting, otherwise a freshly generated event, otherwise null. */
    @Nullable
    private EventSplit nextSplit() {
        EventSplit recycled = pendingSplits.poll();
        if (recycled != null) {
            return recycled;
        }
        if (config.bounded() && generatedEvents >= config.maxEvents()) {
            return null;
        }
        RandomEvent event =
                new RandomEvent("event-" + generatedEvents, random.nextLong(), config.recordsPerEvent());
        generatedEvents++;
        return new EventSplit(event);
    }

    @Override
    public EnumeratorState snapshotState(long checkpointId) {
        return new EnumeratorState(generatedEvents, new ArrayList<>(pendingSplits));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Event generator closing after producing {} event(s)", generatedEvents);
    }
}
