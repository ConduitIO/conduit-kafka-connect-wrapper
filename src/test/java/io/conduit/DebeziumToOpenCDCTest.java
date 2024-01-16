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

import java.io.IOException;
import java.io.InputStream;

import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumToOpenCDCTest {
    private DebeziumToOpenCDC underTest;
    private Schema keySchema;
    private Schema valueSchema;

    @BeforeEach
    void setUp() {
        underTest = new DebeziumToOpenCDC();

        keySchema = new SchemaBuilder(Schema.Type.STRUCT)
            .name("customer_id_schema")
            .field("id", Schema.INT32_SCHEMA)
            .build();

        valueSchema = new SchemaBuilder(Schema.Type.STRUCT)
            .name("customers")
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("interests", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("trial", SchemaBuilder.BOOLEAN_SCHEMA)
            .field("balance", Schema.FLOAT64_SCHEMA)
            .build();

    }

    @Test
    void noValue() {
        var e = assertThrows(
            IllegalArgumentException.class,
            () -> underTest.apply(new SourceRecord(
                null,
                null,
                "test-topic",
                keySchema,
                new Struct(keySchema).put("id", 123),
                valueSchema,
                null))
        );
        assertEquals("record has no value", e.getMessage());
    }

    @Test
    void valueNotStruct() {
        var e = assertThrows(
            IllegalArgumentException.class,
            () -> underTest.apply(new SourceRecord(
                null,
                null,
                "test-topic",
                keySchema,
                new Struct(keySchema).put("id", 123),
                Schema.STRING_SCHEMA,
                "string value"))
        );
        assertEquals("expected value schema to be STRUCT", e.getMessage());
    }

    @SneakyThrows
    @Test
    void keyPreserved() {
        SchemaAndValue schemaAndValue = getCreatedRecord();

        Record rec = underTest.apply(new SourceRecord(
            null,
            null,
            "test-topic",
            keySchema,
            new Struct(keySchema).put("id", 123),
            schemaAndValue.schema(),
            schemaAndValue.value())).build();

        assertEquals(
            123,
            (int) rec.getKey().getStructuredData().getFieldsOrThrow("id").getNumberValue()
        );
    }

    private SchemaAndValue getCreatedRecord() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("./debezium-record-created.json");
        assertNotNull(stream);
        return Utils.jsonConv.toConnectData("test-topic", IOUtils.toByteArray(stream));
    }

    private SchemaAndValue getUpdatedRecord() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("./debezium-record-updated.json");
        assertNotNull(stream);
        return Utils.jsonConv.toConnectData("test-topic", IOUtils.toByteArray(stream));
    }

    @SneakyThrows
    @Test
    void createdRecord() {
        SchemaAndValue schemaAndValue = getCreatedRecord();

        Struct original = (Struct) schemaAndValue.value();
        Record transformed = underTest.apply(new SourceRecord(
            null,
            null,
            "test-topic",
            keySchema,
            new Struct(keySchema).put("id", 123),
            schemaAndValue.schema(),
            original)
        ).build();

        // Operation
        assertEquals(Operation.OPERATION_CREATE, transformed.getOperation());

        // Before
        assertFalse(transformed.getPayload().getBefore().hasStructuredData());
        assertFalse(transformed.getPayload().getBefore().hasRawData());

        // After
        assertTrue(transformed.getPayload().getAfter().hasStructuredData());
        com.google.protobuf.Struct after = transformed.getPayload().getAfter().getStructuredData();
        assertContentsMatch(original.getStruct("after"), after);

        // Metadata
        assertMetadataOk(original, transformed);
    }

    @SneakyThrows
    @Test
    void updatedRecord() {
        SchemaAndValue schemaAndValue = getUpdatedRecord();

        Struct original = (Struct) schemaAndValue.value();
        Record transformed = underTest.apply(new SourceRecord(
            null,
            null,
            "test-topic",
            keySchema,
            new Struct(keySchema).put("id", 123),
            schemaAndValue.schema(),
            original)
        ).build();

        // Operation
        assertEquals(Operation.OPERATION_UPDATE, transformed.getOperation());

        // Before
        assertTrue(transformed.getPayload().getBefore().hasStructuredData());
        com.google.protobuf.Struct before = transformed.getPayload().getBefore().getStructuredData();
        assertContentsMatch(original.getStruct("before"), before);

        // After
        assertTrue(transformed.getPayload().getAfter().hasStructuredData());
        com.google.protobuf.Struct after = transformed.getPayload().getAfter().getStructuredData();
        assertContentsMatch(original.getStruct("after"), after);

        // Metadata
        assertMetadataOk(original, transformed);
    }

    private void assertContentsMatch(Struct afterOrig, com.google.protobuf.Struct after) {
        assertEquals((int) afterOrig.getInt32("id"), after.getFieldsOrThrow("id").getNumberValue());
        assertEquals(afterOrig.getString("name"), after.getFieldsOrThrow("name").getStringValue());
        assertEquals(afterOrig.getBoolean("full_time"), after.getFieldsOrThrow("full_time").getBoolValue());
        assertEquals(afterOrig.getString("updated_at"), after.getFieldsOrThrow("updated_at").getStringValue());
    }

    private void assertMetadataOk(Struct original, Record transformed) {
        Struct source = original.getStruct("source");
        for (Field field : source.schema().fields()) {
            Object fieldVal = source.get(field);
            assertEquals(
                String.valueOf(fieldVal),
                transformed.getMetadataMap().get("kafkaconnect.debezium.source." + field.name())
            );
        }
    }
}
