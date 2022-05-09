package io.conduit;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

// todo add test for unknown types
public class RawDataSchemaProviderTest {
    @Test
    public void testFull() {
        RawDataSchemaProvider underTest = new RawDataSchemaProvider("myschema", null);
        ObjectNode json = Utils.mapper.createObjectNode()
                .put("byteField", Byte.MAX_VALUE)
                .put("shortField", Short.MAX_VALUE)
                .put("intField", Integer.MAX_VALUE)
                .put("longField", Long.MAX_VALUE)
                .put("stringField", "test string")
                .put("bytesField", "test bytes".getBytes(StandardCharsets.UTF_8))
                .put("floatField", 12.34f)
                .put("doubleField", 12.34d)
                .put("boolField", true)
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
        assertEquals(Schema.Type.INT8, result.field("byteField").schema().type());
        assertEquals(Schema.Type.INT16, result.field("shortField").schema().type());
        assertEquals(Schema.Type.INT32, result.field("intField").schema().type());
        assertEquals(Schema.Type.INT64, result.field("longField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("floatField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("doubleField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringField").schema().type());
        // bytes are Base64 encoded
        assertEquals(Schema.Type.STRING, result.field("bytesField").schema().type());
        assertEquals(Schema.Type.BOOLEAN, result.field("boolField").schema().type());
        assertEquals(Schema.Type.ARRAY, result.field("stringArrayField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringArrayField").schema().valueSchema().type());
    }

    @Test
    public void testNonJson() {
        RawDataSchemaProvider underTest = new RawDataSchemaProvider("myschema", null);
        Record record = Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8("test-key")).build())
                .setPayload(Data.newBuilder()
                        .setRawData(ByteString.copyFrom(new byte[]{11, 22, 33}))
                        .build()
                ).build();
        Schema schema = underTest.provide(record);
        assertNotNull(schema);
        assertEquals("myschema", schema.name());
        assertEquals(Schema.Type.BYTES, schema.type());
    }

    @Test
    public void testNullRecord() {
        RawDataSchemaProvider underTest = new RawDataSchemaProvider("myschema", null);
        assertNull(underTest.provide(null));
    }

    @Test
    public void testNoPayload() {
        RawDataSchemaProvider underTest = new RawDataSchemaProvider("myschema", null);
        Record record = Record.newBuilder().build();
        assertNull(underTest.provide(record));
    }

    @Test
    public void testStructPayload() {
        RawDataSchemaProvider underTest = new RawDataSchemaProvider("myschema", null);
        Record record = Record.newBuilder()
                .setPayload(
                        Data.newBuilder().setStructuredData(Struct.newBuilder().build()).build()
                ).build();
        var e = assertThrows(IllegalArgumentException.class, () -> underTest.provide(record));
        assertEquals("Record has no raw data.", e.getMessage());
    }
}
