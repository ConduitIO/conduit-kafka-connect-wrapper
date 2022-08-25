package io.conduit;

import com.google.protobuf.*;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StructSchemaProviderTest {
    @Test
    public void testNumbers() {
        StructSchemaProvider underTest = new StructSchemaProvider("myschema", null);
        var struct = Struct.newBuilder()
                .putFields("byteField", Value.newBuilder().setNumberValue((byte) 5).build())
                .putFields("shortField", Value.newBuilder().setNumberValue((short) 25).build())
                .putFields("intField", Value.newBuilder().setNumberValue(123).build())
                .putFields("longField", Value.newBuilder().setNumberValue(Long.MAX_VALUE).build())
                .putFields("floatField", Value.newBuilder().setNumberValue(12.34f).build())
                .putFields("doubleField", Value.newBuilder().setNumberValue(12.34d).build())
                .build();

        Record record = toRecord(struct);

        Schema result = underTest.provide(record);
        assertEquals("myschema", result.name());
        assertEquals(struct.getFieldsCount(), result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());

        assertEquals(Schema.Type.FLOAT64, result.field("byteField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("shortField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("intField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("longField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("floatField").schema().type());
        assertEquals(Schema.Type.FLOAT64, result.field("doubleField").schema().type());
    }

    @Test
    public void testNullField() {
        StructSchemaProvider underTest = new StructSchemaProvider("myschema", null);
        var struct = Struct.newBuilder()
                .putFields("nullValueField", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .build();

        Record record = toRecord(struct);

        Schema result = underTest.provide(record);
        assertEquals("myschema", result.name());
        // not counting the null field
        assertEquals(0, result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());
    }

    @Test
    public void testString() {
        StructSchemaProvider underTest = new StructSchemaProvider("myschema", null);
        var struct = Struct.newBuilder()
                .putFields("stringField", Value.newBuilder().setStringValue("test string").build())
                .build();

        Record record = toRecord(struct);

        Schema result = underTest.provide(record);
        assertEquals("myschema", result.name());
        // not counting the null field
        assertEquals(struct.getFieldsCount(), result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());
        assertEquals(Schema.Type.STRING, result.field("stringField").schema().type());
    }

    @Test
    public void testStringArray() {
        StructSchemaProvider underTest = new StructSchemaProvider("myschema", null);
        var struct = Struct.newBuilder()
                .putFields(
                        "stringArrayField",
                        Value.newBuilder()
                                .setListValue(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("a").build()))
                                .build()
                ).build();

        Record record = toRecord(struct);

        Schema result = underTest.provide(record);
        assertEquals("myschema", result.name());
        // not counting the null field
        assertEquals(struct.getFieldsCount(), result.fields().size());
        assertEquals(Schema.Type.STRUCT, result.type());
        assertEquals(Schema.Type.ARRAY, result.field("stringArrayField").schema().type());
        assertEquals(Schema.Type.STRING, result.field("stringArrayField").schema().valueSchema().type());
    }

    private Record toRecord(Struct struct) {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8("test-key")).build())
                .setPayload(TestUtils.newCreatedRecord(struct))
                .build();
    }
}
