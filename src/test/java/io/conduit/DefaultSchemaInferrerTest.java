package io.conduit;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultSchemaInferrerTest {
    @Test
    public void testRawJsonData() {
        DefaultSchemaProvider underTest = new DefaultSchemaProvider();
        ObjectNode json = Utils.mapper.createObjectNode()
                .put("byteField", (byte) 5)
                .put("shortField", (short) 25)
                .put("intField", 123)
                .put("longField", Long.MAX_VALUE)
                .put("stringField", "test string")
                .put("bytesField", "test bytes".getBytes(StandardCharsets.UTF_8))
                .put("floatField", 12.34f)
                .put("doubleField", 12.34d)
                .set("arrayField", Utils.mapper.createArrayNode().add("a").add("b").add("c"));

        Record record = Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8("test-key")).build())
                .setPayload(Data.newBuilder()
                        .setRawData(ByteString.copyFromUtf8(json.toString()))
                        .build()
                ).build();

        Schema result = underTest.provide(record, "myschema");

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
        assertEquals(Schema.Type.ARRAY, result.field("arrayField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("arrayField").schema().valueSchema().type());
    }

    @Test
    public void testStructuredData() {
        DefaultSchemaProvider underTest = new DefaultSchemaProvider();
        var struct = Struct.newBuilder()
                .putFields("byteField", Value.newBuilder().setNumberValue((byte) 5).build())
                .putFields("shortField", Value.newBuilder().setNumberValue((short) 25).build())
                .putFields("intField", Value.newBuilder().setNumberValue(123).build())
                .putFields("longField", Value.newBuilder().setNumberValue(Long.MAX_VALUE).build())
                .putFields("stringField", Value.newBuilder().setStringValue("test string").build())
                // .putFields("bytesField", Value.newBuilder().set.build())
                .putFields("floatField", Value.newBuilder().setNumberValue(12.34f).build())
                .putFields("doubleField", Value.newBuilder().setNumberValue(12.34d).build())
                .putFields(
                        "arrayField",
                        Value.newBuilder()
                                .setListValue(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("a").build()))
                                .build()
                );

        Record record = Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8("test-key")).build())
                .setPayload(Data.newBuilder()
                        .setStructuredData(struct)
                        .build()
                ).build();

        Schema result = underTest.provide(record, "myschema");
        assertEquals("myschema", result.name());
        assertEquals(struct.getFieldsCount(), result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());
        // When parsing raw JSON, we interpret ints as longs
        assertEquals(Schema.Type.FLOAT64, result.field("byteField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("shortField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("intField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("longField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("floatField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("doubleField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringField").schema().type());
        // assertEquals(Schema.Type.BYTES, result.field("bytesField").schema().type());
        assertEquals(Schema.Type.ARRAY, result.field("arrayField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("arrayField").schema().valueSchema().type());
    }
}
