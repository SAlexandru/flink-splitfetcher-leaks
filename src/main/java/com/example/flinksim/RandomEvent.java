package com.example.flinksim;

import java.io.Serializable;

/**
 * One unit of work produced by the generator.
 *
 * <p>In the connector this simulates, an event arrives on a notification topic and points at an
 * object in remote storage. Here there is no object: {@code seed} and {@code recordCount} fully
 * describe the data a reader will synthesize, so a reader can reproduce the exact same records
 * without any I/O.
 *
 * @param eventId unique id; also used as the split id
 * @param seed seed for the payload generator, so a split is reproducible after a restore
 * @param recordCount how many records this event expands into
 */
public record RandomEvent(String eventId, long seed, int recordCount) implements Serializable {}
