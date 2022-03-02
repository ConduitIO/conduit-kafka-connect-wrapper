package io.conduit;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import static io.conduit.Transformations.fromKafkaSource;
import static org.junit.jupiter.api.Assertions.*;

public class TransformationsTest {
    private Schema testSchema;
    private Struct testValue;
    private ObjectNode schemaJson;
    private JsonNode testRecord;

    @BeforeEach
    public void setUp() {
        testSchema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customers")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("interests", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("trial", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("balance", Schema.FLOAT64_SCHEMA)
                .build();
        schemaJson = Utils.jsonConv.asJsonSchema(testSchema);
        testValue = new Struct(testSchema)
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
                testSchema,
                testValue
        );
        Record.Builder conduitRec = Transformations.fromKafkaSource(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getStructuredData();
        assertMatch(testValue, payload);
        // assert timestamp is within last second
        assertTrue(
                conduitRec.getCreatedAt().getSeconds() * 1000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertFalse(conduitRec.getKey().hasStructuredData());
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
                .map(v -> v.getStringValue())
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
        assertEquals("record has no payload", e.getMessage());
    }

    @Test
    public void testToSinkRecord_RawData() {
        var rec = newRecordRawData();
        var sinkRecObj = Transformations.toConnectData(rec, schemaJson);
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

        verifySinkRecord(Transformations.toConnectData(rec, schemaJson));
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
                .setCreatedAt(Timestamp.newBuilder().setSeconds(123456).build())
                .build();
    }

    @SneakyThrows
    private Data newStructPayload() {
        com.google.protobuf.Struct.Builder builder = com.google.protobuf.Struct.newBuilder();
        JsonFormat.parser().merge(
                Utils.mapper.writeValueAsString(testRecord),
                builder
        );
        return Data.newBuilder()
                .setStructuredData(builder.build())
                .build();
    }

    private Record newRecordRawData() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(newRawPayload())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(123456).build())
                .build();
    }

    @SneakyThrows
    private Data newRawPayload() {
        return Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8(Utils.mapper.writeValueAsString(testRecord)))
                .build();
    }
}
