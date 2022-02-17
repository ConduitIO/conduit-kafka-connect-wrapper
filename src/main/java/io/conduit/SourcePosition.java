package io.conduit;

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode
public class SourcePosition {
    // Keys are partitions as JSON strings (so that they can be easily deserialized),
    // and values are offsets, as defined by the Kafka Connect API.
    // For more information, see the documentation
    private final Map<SourcePartition, SourceOffset> positions = new HashMap<>();

    @SneakyThrows
    public void add(Map<String, ?> partition, Map<String, Object> offset) {
        positions.put(new SourcePartition(partition), new SourceOffset(offset));
    }

    @SneakyThrows
    public byte[] jsonBytes() {
        return Utils.mapper.writeValueAsBytes(positions);
    }

    public SourceOffset offsetFor(Map<String, ?> partition) {
        SourceOffset offset = positions.get(new SourcePartition(partition));
        return offset != null ? offset : new SourceOffset();
    }
}
