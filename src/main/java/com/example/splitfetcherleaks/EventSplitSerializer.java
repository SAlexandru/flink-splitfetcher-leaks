package com.example.splitfetcherleaks;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializes {@link EventSplit} for checkpoints and for shipping splits to readers. */
public final class EventSplitSerializer implements SimpleVersionedSerializer<EventSplit> {

    public static final EventSplitSerializer INSTANCE = new EventSplitSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(EventSplit split) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            RandomEvent event = split.event();
            out.writeUTF(event.eventId());
            out.writeLong(event.seed());
            out.writeInt(event.recordCount());
            out.writeInt(split.emittedCount());
            out.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public EventSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported split serializer version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String eventId = in.readUTF();
            long seed = in.readLong();
            int recordCount = in.readInt();
            int emittedCount = in.readInt();
            return new EventSplit(new RandomEvent(eventId, seed, recordCount), emittedCount);
        }
    }
}
