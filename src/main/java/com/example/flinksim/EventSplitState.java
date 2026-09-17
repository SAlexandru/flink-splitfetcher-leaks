package com.example.flinksim;

/**
 * Mutable reading progress for one split.
 *
 * <p>{@link EventSplit} is immutable so it can be shipped and checkpointed safely; the reader needs
 * something it can update per record without allocating, which is what this holds. It is converted
 * back into an {@link EventSplit} when a checkpoint is taken.
 */
public final class EventSplitState {

    private final RandomEvent event;
    private int emittedCount;

    public EventSplitState(EventSplit split) {
        this.event = split.event();
        this.emittedCount = split.emittedCount();
    }

    public void setEmittedCount(int emittedCount) {
        this.emittedCount = emittedCount;
    }

    public EventSplit toEventSplit() {
        return new EventSplit(event, emittedCount);
    }
}
