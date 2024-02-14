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

import com.fasterxml.jackson.databind.JsonNode;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumToOpenCDCTest {
    private Schema keySchema;
    private Schema valueSchema;

    @BeforeEach
    void setUp() {
        keySchema = new SchemaBuilder(Schema.Type.STRUCT)
                .name("customer_id_schema")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
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
        var underTest = new DebeziumToOpenCDC(false);

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
        var underTest = new DebeziumToOpenCDC(false);

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
        var underTest = new DebeziumToOpenCDC(false);

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

    @SneakyThrows
    @Test
    void createdRecord() {
        SchemaAndValue schemaAndValue = getCreatedRecord();
        var underTest = new DebeziumToOpenCDC(false);

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
        assertMetadataOk(original, transformed, false);
    }

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void schemaSaved(boolean saved) {
        SchemaAndValue schemaAndValue = getCreatedRecord();
        var underTest = new DebeziumToOpenCDC(saved);

        Struct original = (Struct) schemaAndValue.value();
        Struct key = new Struct(keySchema).put("id", 123);
        Record transformed = underTest.apply(new SourceRecord(
                null,
                null,
                "test-topic",
                keySchema,
                key,
                schemaAndValue.schema(),
                original)
        ).build();

        // Metadata
        assertMetadataOk(original, transformed, false);
        assertValueSchemaOk(schemaAndValue, transformed, saved);
        assertKeySchemaOk(key, transformed, saved);
    }

    @SneakyThrows
    @Test
    void updatedRecord() {
        SchemaAndValue schemaAndValue = getUpdatedRecord();
        var underTest = new DebeziumToOpenCDC(false);

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
        assertMetadataOk(original, transformed, false);
    }

    @SneakyThrows
    @Test
    void deletedRecord() {
        SchemaAndValue schemaAndValue = getDeletedRecord();
        var underTest = new DebeziumToOpenCDC(true);

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
        assertEquals(Operation.OPERATION_DELETE, transformed.getOperation());

        // Before
        assertTrue(transformed.getPayload().getBefore().hasStructuredData());
        com.google.protobuf.Struct before = transformed.getPayload().getBefore().getStructuredData();
        assertContentsMatch(original.getStruct("before"), before);

        // After
        assertFalse(transformed.getPayload().getAfter().hasStructuredData());
        assertNull(original.getStruct("after"));
        com.google.protobuf.Struct after = transformed.getPayload().getAfter().getStructuredData();
        assertEquals(after, com.google.protobuf.Struct.getDefaultInstance());

        // Metadata
        assertMetadataOk(original, transformed, true);
    }

    @SneakyThrows
    @Test
    /**
     * Tests the absence of both before and after, which is unexpected
     */
    void invalidRecord() {
        SchemaAndValue schemaAndValue = getInvalidRecord();
        var underTest = new DebeziumToOpenCDC(true);

        Struct original = (Struct) schemaAndValue.value();
        SourceRecord sr = new SourceRecord(
                null,
                null,
                "test-topic",
                keySchema,
                new Struct(keySchema).put("id", 123),
                schemaAndValue.schema(),
                original);
        assertThrows(
                IllegalArgumentException.class,
                () -> underTest.apply(sr)
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

    private SchemaAndValue getDeletedRecord() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("./debezium-record-deleted.json");
        assertNotNull(stream);
        return Utils.jsonConv.toConnectData("test-topic", IOUtils.toByteArray(stream));
    }

    private SchemaAndValue getInvalidRecord() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("./debezium-record-invalid.json");
        assertNotNull(stream);
        return Utils.jsonConv.toConnectData("test-topic", IOUtils.toByteArray(stream));
    }

    private void assertContentsMatch(Struct afterOrig, com.google.protobuf.Struct after) {
        assertEquals((int) afterOrig.getInt32("id"), after.getFieldsOrThrow("id").getNumberValue());
        assertEquals(afterOrig.getString("name"), after.getFieldsOrThrow("name").getStringValue());
        assertEquals(afterOrig.getBoolean("full_time"), after.getFieldsOrThrow("full_time").getBoolValue());
        assertEquals(afterOrig.getString("updated_at"), after.getFieldsOrThrow("updated_at").getStringValue());
    }

    private void assertMetadataOk(Struct original, Record transformed, Boolean skipAfter) {
        Struct source = original.getStruct("source");
        for (Field field : source.schema().fields()) {
            if (skipAfter && field.name() == "after") {
                continue;
            }
            Object fieldVal = source.get(field);
            assertEquals(
                    String.valueOf(fieldVal),
                    transformed.getMetadataMap().get("kafkaconnect.debezium.source." + field.name())
            );
        }
    }

    @SneakyThrows
    private void assertValueSchemaOk(SchemaAndValue schemaValue, Record record, boolean saved) {
        assertEquals(
                saved,
                record.getMetadataMap().containsKey("kafkaconnect.value.schema")
        );
        if (!saved) {
            return;
        }

        JsonNode actual = Utils.mapper.readTree(record.getMetadataMap().get("kafkaconnect.value.schema"));

        Schema schema = ((Struct) schemaValue.value()).getStruct("after").schema();
        for (Field f : schema.fields()) {
            assertEquals(
                    f.schema().type().toString(),
                    actual.get(f.name()).asText()
            );
        }
    }

    @SneakyThrows
    private void assertKeySchemaOk(Struct key, Record record, boolean saved) {
        assertEquals(
                saved,
                record.getMetadataMap().containsKey("kafkaconnect.key.schema")
        );
        if (!saved) {
            return;
        }

        JsonNode actual = Utils.mapper.readTree(record.getMetadataMap().get("kafkaconnect.key.schema"));

        for (Field f : key.schema().fields()) {
            assertEquals(
                    f.schema().type().toString(),
                    actual.get(f.name()).asText()
            );
        }
    }
}
