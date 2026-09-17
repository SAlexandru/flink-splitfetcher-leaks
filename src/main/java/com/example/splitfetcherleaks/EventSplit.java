package com.example.splitfetcherleaks;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * A single event turned into a source split.
 *
 * <p>Splits are never divided: one event is one split, mirroring a non-splitting file enumerator
 * that treats every object as an indivisible unit of work.
 *
 * <p>{@code emittedCount} is the resumable part of the state. After a restore the reader skips the
 * records that were already emitted rather than replaying them.
 */
public final class EventSplit implements SourceSplit {

    private final RandomEvent event;
    private final int emittedCount;

    public EventSplit(RandomEvent event) {
        this(event, 0);
    }

    public EventSplit(RandomEvent event, int emittedCount) {
        this.event = event;
        this.emittedCount = emittedCount;
    }

    @Override
    public String splitId() {
        return event.eventId();
    }

    public RandomEvent event() {
        return event;
    }

    public int emittedCount() {
        return emittedCount;
    }

    /** How many records are still owed by this split. */
    public int remaining() {
        return event.recordCount() - emittedCount;
    }

    public EventSplit withEmittedCount(int newEmittedCount) {
        return new EventSplit(event, newEmittedCount);
    }

    @Override
    public String toString() {
        return "EventSplit{" + event.eventId() + ", " + emittedCount + "/" + event.recordCount() + "}";
    }
}
