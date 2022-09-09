/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ToConnectDataTest {
    private ToConnectData underTest;
    private Schema valueSchema;
    private JsonNode testRecord;

    @BeforeEach
    public void setUp() {
        underTest = new ToConnectData();

        valueSchema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customers")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("interests", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .field("trial", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("balance", Schema.FLOAT64_SCHEMA)
                .build();

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
                () -> underTest.apply(null, null)
        );
        assertEquals("record is null", e.getMessage());
    }

    @Test
    public void testToSinkRecord_NoPayload() {
        var rec = Record.newBuilder().build();
        var e = assertThrows(
                IllegalArgumentException.class,
                () -> underTest.apply(rec, null)
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
        var sinkRecObj = underTest.apply(rec, schema);
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
        var sinkRecObj = underTest.apply(rec, schema);
        assertInstanceOf(String.class, sinkRecObj);
        assertEquals(rec.getPayload().getAfter().getRawData().toStringUtf8(), sinkRecObj);
    }

    @Test
    public void testToSinkRecord_RawDataJson() {
        var rec = newRecordRawDataJson();
        var sinkRecObj = underTest.apply(rec, valueSchema);
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
                () -> underTest.apply(rec, null)
        );
        assertEquals(
                "cannot parse struct without schema",
                e.getMessage()
        );
    }

    @Test
    public void testToSinkRecord_StructuredData() {
        var rec = newRecordStructData();

        verifySinkRecord(underTest.apply(rec, valueSchema));
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
                .putMetadata(OpenCdcMetadata.CREATED_AT, "123456000000000")
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
                .putMetadata(OpenCdcMetadata.CREATED_AT, "123456000000000")
                .build();
    }

    private Record newRecordRawData() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(newRawPayload())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .putMetadata(OpenCdcMetadata.CREATED_AT, "123456000000000")
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
