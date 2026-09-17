package com.example.splitfetcherleaks;

import java.io.Serializable;

/**
 * A record synthesized by a reader, standing in for one deserialized message read out of a remote
 * object.
 *
 * @param splitId the split that produced this record
 * @param sequence 0-based position within the split
 * @param payload random bytes, sized to make the reader do real work
 */
public record RandomRecord(String splitId, int sequence, byte[] payload) implements Serializable {

    @Override
    public String toString() {
        return "RandomRecord{" + splitId + "#" + sequence + ", " + payload.length + " bytes}";
    }
}
