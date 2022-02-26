package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SourcePositionTest {
    @Test
    public void testEmpty() {
        SourcePosition original = new SourcePosition();
        String jsonString = original.asByteString().toStringUtf8();
        assertEquals(
                original,
                SourcePosition.fromString(jsonString)
        );
    }

    @Test
    public void testNonEmpty() {
        SourcePosition original = new SourcePosition();
        original.add(
                Map.of("partition1", "partition1-value"),
                Map.of("offset1", "offset1-value")
        );
        original.add(
                Map.of("partition2", "partition2-value"),
                Map.of("offset2", "offset2-value")
        );
        String jsonString = original.asByteString().toStringUtf8();
        assertEquals(
                original,
                SourcePosition.fromString(jsonString)
        );
    }
}