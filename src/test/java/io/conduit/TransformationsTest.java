package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.conduit.Transformations.fromKafkaSource;
import static org.junit.jupiter.api.Assertions.*;

public class TransformationsTest {
    private Schema valueSchema;
    private Schema keySchema;
    private Struct testValue;
    private JsonNode testRecord;

    @BeforeEach
    public void setUp() {
        valueSchema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customers")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("interests", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("trial", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("balance", Schema.FLOAT64_SCHEMA)
                .build();
        keySchema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customer_id_schema")
                .field("id", Schema.INT32_SCHEMA)
                .build();

        testValue = new Struct(valueSchema)
                .put("id", 123)
                .put("name", "foobar")
                .put("trial", true)
                .put("balance", 33.44)
                .put("interests", List.of("aaa", "bbb"));
        testRecord = Utils.mapper.createObjectNode()
                .put("id", 123)
                .put("name", "foobar")
                .put("trial", true)
                .put("balance", 33.44)
                .set("interests", Utils.mapper.createArrayNode().add("aaa").add("bbb"));
    }

    @Test
    public void testFromKafkaSource_Null() {
        assertNull(fromKafkaSource(null));
    }

    @SneakyThrows
    @Test
    public void testFromKafkaSource_WithValueSchema_NoKeySchema() {
        var sourceRecord = new SourceRecord(
                Map.of("test-partition", "test_table"),
                Map.of("test-offset", 123456L),
                "test-topic",
                2,
                valueSchema,
                testValue
        );
        Record.Builder conduitRec = Transformations.fromKafkaSource(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getAfter().getStructuredData();
        assertMatch(testValue, payload);
        // assert timestamp is within last second
        long readAt = Long.parseLong(conduitRec.getMetadataOrThrow(OpenCdcMetadata.READ_AT));
        assertTrue(
                readAt / 1_000_000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertFalse(conduitRec.getKey().hasStructuredData());
    }

    @SneakyThrows
    @Test
    public void testFromKafkaSource_WithValueSchema_WithKeySchema() {
        var sourceRecord = new SourceRecord(
                Map.of("test-partition", "test_table"),
                Map.of("test-offset", 123456L),
                "test-topic",
                2,
                keySchema,
                new Struct(keySchema).put("id", 123),
                valueSchema,
                testValue
        );
        Record.Builder conduitRec = Transformations.fromKafkaSource(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getAfter().getStructuredData();
        assertMatch(testValue, payload);
        // assert timestamp is within last second
        long readAt = Long.parseLong(conduitRec.getMetadataOrThrow(OpenCdcMetadata.READ_AT));
        assertTrue(
                readAt / 1_000_000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertTrue(conduitRec.getKey().hasStructuredData());
        com.google.protobuf.Struct key = conduitRec.getKey().getStructuredData();
        assertEquals(123, key.getFieldsOrThrow("id").getNumberValue());
    }

    private void assertMatch(Struct expected, com.google.protobuf.Struct payload) {
        assertEquals(expected.get("id"), (int) payload.getFieldsOrThrow("id").getNumberValue());
        assertEquals(expected.get("name"), payload.getFieldsOrThrow("name").getStringValue());
        assertEquals(expected.get("trial"), payload.getFieldsOrThrow("trial").getBoolValue());
        assertEquals(expected.get("balance"), payload.getFieldsOrThrow("balance").getNumberValue());
        List<String> interestsExpected = expected.getArray("interests");
        List<String> interestsActual = payload.getFieldsOrThrow("interests")
                .getListValue().getValuesList()
                .stream()
                .map(Value::getStringValue)
                .collect(Collectors.toList());
        assertEquals(interestsExpected, interestsActual);
    }

    @Test
    public void testToSinkRecord_NoRecord() {
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> Transformations.toConnectData(null, null)
        );
        assertEquals("record is null", e.getMessage());
    }

    @Test
    public void testToSinkRecord_NoPayload() {
        var rec = Record.newBuilder().build();
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> Transformations.toConnectData(rec, null)
        );
        assertEquals("record has no payload or has no after data", e.getMessage());
    }

    @Test
    public void testToSinkRecord_RawDataBytes() {
        var schema = new SchemaBuilder(Schema.Type.BYTES)
                .name("my-bytes-schema")
                .optional()
                .build();
        var rec = newRecordRawData();
        var sinkRecObj = Transformations.toConnectData(rec, schema);
        assertInstanceOf(byte[].class, sinkRecObj);
        assertArrayEquals(rec.getPayload().getAfter().getRawData().toByteArray(), (byte[]) sinkRecObj);
    }

    @Test
    public void testToSinkRecord_RawDataString() {
        var schema = new SchemaBuilder(Schema.Type.STRING)
                .name("my-string-schema")
                .optional()
                .build();
        var rec = newRecordRawData();
        var sinkRecObj = Transformations.toConnectData(rec, schema);
        assertInstanceOf(String.class, sinkRecObj);
        assertEquals(rec.getPayload().getAfter().getRawData().toStringUtf8(), sinkRecObj);
    }

    @Test
    public void testToSinkRecord_RawDataJson() {
        var rec = newRecordRawDataJson();
        var sinkRecObj = Transformations.toConnectData(rec, valueSchema);
        assertInstanceOf(Struct.class, sinkRecObj);

        Struct value = (Struct) sinkRecObj;
        assertEquals(testRecord.get("id").asInt(), value.get("id"));
        assertEquals(testRecord.get("name").asText(), value.get("name"));
        assertEquals(List.of("aaa", "bbb"), value.get("interests"));
        assertEquals(testRecord.get("trial").asBoolean(), value.get("trial"));
        assertEquals(testRecord.get("balance").asDouble(), value.get("balance"));
    }

    @Test
    public void testToSinkRecord_StructuredData_NoSchema() {
        var rec = newRecordStructData();

        var e = assertThrows(
                IllegalArgumentException.class,
                () -> Transformations.toConnectData(rec, null)
        );
        assertEquals(
                "cannot parse struct without schema",
                e.getMessage()
        );
    }

    @Test
    public void testToSinkRecord_StructuredData() {
        var rec = newRecordStructData();

        verifySinkRecord(Transformations.toConnectData(rec, valueSchema));
    }

    public void verifySinkRecord(Object actualObj) {
        assertInstanceOf(Struct.class, actualObj);

        Struct value = (Struct) actualObj;
        assertEquals(123, value.get("id"));
        assertEquals("foobar", value.get("name"));
    }

    private Record newRecordStructData() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(newStructPayload())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .putMetadata(OpenCdcMetadata.READ_AT, "123456000000000")
                .build();
    }

    @SneakyThrows
    private Change newStructPayload() {
        com.google.protobuf.Struct.Builder builder = com.google.protobuf.Struct.newBuilder();
        JsonFormat.parser().merge(
                Utils.mapper.writeValueAsString(testRecord),
                builder
        );
        Data data = Data.newBuilder()
                .setStructuredData(builder.build())
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }

    private Record newRecordRawDataJson() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(newRawPayloadJson())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .putMetadata(OpenCdcMetadata.READ_AT, "123456000000000")
                .build();
    }

    private Record newRecordRawData() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(newRawPayload())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .putMetadata(OpenCdcMetadata.READ_AT, "123456000000000")
                .build();
    }

    @SneakyThrows
    private Change newRawPayload() {
        Data data = Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("payload-" + UUID.randomUUID()))
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }

    @SneakyThrows
    private Change newRawPayloadJson() {
        Data data = Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8(Utils.mapper.writeValueAsString(testRecord)))
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }
}
