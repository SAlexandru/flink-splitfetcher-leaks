package com.example.flinksim;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializes {@link EnumeratorState}, delegating the split entries to {@link EventSplitSerializer}. */
public final class EnumeratorStateSerializer implements SimpleVersionedSerializer<EnumeratorState> {

    public static final EnumeratorStateSerializer INSTANCE = new EnumeratorStateSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(EnumeratorState state) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(state.generatedEvents());
            out.writeInt(state.pendingSplits().size());
            for (EventSplit split : state.pendingSplits()) {
                byte[] serializedSplit = EventSplitSerializer.INSTANCE.serialize(split);
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }
            out.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public EnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported enumerator state version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            long generatedEvents = in.readLong();
            int splitCount = in.readInt();
            List<EventSplit> pendingSplits = new ArrayList<>(splitCount);
            for (int i = 0; i < splitCount; i++) {
                byte[] splitBytes = new byte[in.readInt()];
                in.readFully(splitBytes);
                pendingSplits.add(
                        EventSplitSerializer.INSTANCE.deserialize(
                                EventSplitSerializer.INSTANCE.getVersion(), splitBytes));
            }
            return new EnumeratorState(generatedEvents, pendingSplits);
        }
    }
}
