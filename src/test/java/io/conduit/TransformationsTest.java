package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationsTest {
    private ObjectNode schemaJson;
    private JsonNode testRecord;

    @BeforeEach
    public void setUp() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customers")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("interests", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("trial", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("balance", Schema.FLOAT64_SCHEMA)
                .build();
        schemaJson = Utils.jsonConv.asJsonSchema(schema);
        testRecord = Utils.mapper.createObjectNode()
                .put("id", 123)
                .put("name", "foobar")
                .put("trial", true)
                .put("balance", 33.44)
                .set("interests", Utils.mapper.createArrayNode().add("aaa").add("bbb"));
    }

    @Test
    public void testToSinkRecord_NoRecord() {
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> Transformations.toStruct(null, null)
        );
        assertEquals("record is null", e.getMessage());
    }

    @Test
    public void testToSinkRecord_NoPayload() {
        var rec = Record.newBuilder().build();
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> Transformations.toStruct(rec, null)
        );
        assertEquals("record has no payload", e.getMessage());
    }

    @Test
    public void testToSinkRecord_RawData() {
        var rec = newRecordRawData();
        var sinkRecObj = Transformations.toStruct(rec, schemaJson);
        assertInstanceOf(Struct.class, sinkRecObj);

        Struct value = (Struct) sinkRecObj;
        assertEquals(testRecord.get("id").asInt(), value.get("id"));
        assertEquals(testRecord.get("name").asText(), value.get("name"));
        assertEquals(List.of("aaa", "bbb"), value.get("interests"));
        assertEquals(testRecord.get("trial").asBoolean(), value.get("trial"));
        assertEquals(testRecord.get("balance").asDouble(), value.get("balance"));
    }

    @Test
    public void testToSinkRecord_StructuredData() {
        var rec = newRecordStructData();

        verifySinkRecord(Transformations.toStruct(rec, schemaJson));
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
        com.google.protobuf.Struct struct = com.google.protobuf.Struct.parseFrom(
                ByteString.copyFromUtf8(Utils.mapper.writeValueAsString(testRecord))
        );
        return Data.newBuilder()
                .setStructuredData(struct)
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
