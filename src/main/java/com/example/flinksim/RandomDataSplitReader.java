package com.example.flinksim;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;

/**
 * Reads a split by making the data up.
 *
 * <p>Where the real reader opens an object stream and decodes messages until EOF, this one seeds a
 * {@link Random} from the split and produces the requested number of records. Everything around it
 * — batching, marking a split finished, being woken up mid-fetch — behaves the way the connector
 * framework expects, so the surrounding machinery is exercised exactly as it would be against real
 * storage.
 */
public final class RandomDataSplitReader implements SplitReader<RandomRecord, EventSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomDataSplitReader.class);

    private final SimulationConfig config;
    private final Queue<EventSplit> assignedSplits = new ArrayDeque<>();

    /** The split currently being read, along with its generator and progress. */
    private EventSplit current;
    private Random currentRandom;
    private int currentPosition;

    private volatile boolean wokenUp;

    public RandomDataSplitReader(SimulationConfig config) {
        this.config = config;
    }

    @Override
    public RecordsWithSplitIds<RandomRecord> fetch() throws IOException {
        if (current == null && !openNextSplit()) {
            // Nothing assigned yet. Returning an empty batch lets the fetcher loop around and pick
            // up a split assignment without spinning hot.
            parkBriefly();
            return new RecordsBySplits.Builder<RandomRecord>().build();
        }

        RecordsBySplits.Builder<RandomRecord> batch = new RecordsBySplits.Builder<>();
        int produced = 0;
        while (produced < config.batchSize() && current != null) {
            if (currentPosition >= current.event().recordCount()) {
                batch.addFinishedSplit(current.splitId());
                current = null;
                currentRandom = null;
                openNextSplit();
                continue;
            }
            if (wokenUp) {
                // Honour the wakeup: hand back what we have so the fetcher can process the split
                // change that interrupted us.
                wokenUp = false;
                break;
            }

            byte[] payload = new byte[config.payloadBytes()];
            currentRandom.nextBytes(payload);
            batch.add(current.splitId(), new RandomRecord(current.splitId(), currentPosition, payload));
            currentPosition++;
            produced++;
        }

        return batch.build();
    }

    /** Promotes the next assigned split to current. Returns false when none are left. */
    private boolean openNextSplit() {
        EventSplit next = assignedSplits.poll();
        if (next == null) {
            return false;
        }
        current = next;
        currentPosition = next.emittedCount();
        currentRandom = new Random(next.event().seed());
        // Keep the payload stream aligned with the checkpointed position after a restore.
        byte[] skipped = new byte[config.payloadBytes()];
        for (int i = 0; i < currentPosition; i++) {
            currentRandom.nextBytes(skipped);
        }
        LOG.debug("Opened split {} at position {}", next.splitId(), currentPosition);
        return true;
    }

    private void parkBriefly() {
        try {
            Thread.sleep(1L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<EventSplit> splitsChanges) {
        assignedSplits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {
        wokenUp = true;
    }

    @Override
    public void close() throws Exception {
        assignedSplits.clear();
        current = null;
        currentRandom = null;
    }
}
