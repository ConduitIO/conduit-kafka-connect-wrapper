package io.conduit;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.*;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

// todo add test for unknown types
public class JsonSchemaProviderTest {
    @Test
    public void testFull() {
        StructSchemaProvider underTest = new StructSchemaProvider("myschema", null);
        ObjectNode json = Utils.mapper.createObjectNode()
                .put("byteField", (byte) 5)
                .put("shortField", (short) 25)
                .put("intField", 123)
                .put("longField", Long.MAX_VALUE)
                .put("stringField", "test string")
                .put("bytesField", "test bytes".getBytes(StandardCharsets.UTF_8))
                .put("floatField", 12.34f)
                .put("doubleField", 12.34d)
                .set("stringArrayField", Utils.mapper.createArrayNode().add("a").add("b").add("c"));

        Record record = Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8("test-key")).build())
                .setPayload(Data.newBuilder()
                        .setRawData(ByteString.copyFromUtf8(json.toString()))
                        .build()
                ).build();

        Schema result = underTest.provide(record);

        assertEquals("myschema", result.name());
        assertEquals(json.size(), result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());
        // When parsing raw JSON, we interpret ints as longs
        assertEquals(Schema.Type.INT64, result.field("byteField").schema().type());
        assertEquals(Schema.Type.INT64, result.field("shortField").schema().type());
        assertEquals(Schema.Type.INT64, result.field("intField").schema().type());
        assertEquals(Schema.Type.INT64, result.field("longField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("floatField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("doubleField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringField").schema().type());
        // bytes are Base64 encoded
        assertEquals(Schema.Type.STRING, result.field("bytesField").schema().type());
        assertEquals(Schema.Type.ARRAY, result.field("stringArrayField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringArrayField").schema().valueSchema().type());
    }
}
