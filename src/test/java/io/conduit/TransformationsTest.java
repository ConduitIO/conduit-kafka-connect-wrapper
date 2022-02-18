package io.conduit;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TransformationsTest {
    private ObjectNode schemaJson;

    @BeforeEach
    public void setUp() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT)
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        schemaJson = Utils.jsonConv.asJsonSchema(schema);
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
    public void testToSinkRecord_RawJson() {
        var rec = newRecord();
        var sinkRecObj = Transformations.toStruct(rec, schemaJson);
        assertInstanceOf(Struct.class, sinkRecObj);

        Struct value = (Struct) sinkRecObj;
        assertEquals(123, value.get("id"));
        assertEquals("foobar", value.get("name"));
    }

    private Record newRecord() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(Data.newBuilder().setRawData(newRecordPayload()).build())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(123456).build())
                .build();
    }

    private ByteString newRecordPayload() {
        return ByteString.copyFromUtf8(
                "{\"id\":123,\"name\":\"foobar\"}"
        );
    }
}
