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
import java.util.List;

import io.conduit.grpc.Data;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumToOpenCDCTest {
    private DebeziumToOpenCDC underTest;
    private Schema keySchema;
    private Schema valueSchema;
    private Struct testValue;

    @BeforeEach
    public void setUp() {
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

        testValue = new Struct(valueSchema)
            .put("id", 123)
            .put("name", "foobar")
            .put("trial", true)
            .put("balance", 33.44)
            .put("interests", List.of("aaa", "bbb"));

    }

    @Test
    public void noValue() {
        Record rec = underTest.apply(new SourceRecord(
            null,
            null,
            "test-topic",
            keySchema,
            new Struct(keySchema).put("id", 123),
            valueSchema,
            null)).build();

        assertEquals("abc", rec.getKey().getRawData().toStringUtf8());
    }

    @SneakyThrows
    @Test
    public void keyPreserved() {
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
        SchemaAndValue schemaAndValue = Utils.jsonConv.toConnectData("test-topic", IOUtils.toByteArray(stream));
        return schemaAndValue;
    }

    @SneakyThrows
    @Test
    public void createdRecord() {
        SchemaAndValue schemaAndValue = getCreatedRecord();

        Record rec = underTest.apply(new SourceRecord(
            null,
            null,
            "test-topic",
            keySchema,
            new Struct(keySchema).put("id", 123),
            schemaAndValue.schema(),
            schemaAndValue.value())).build();

        assertEquals(Operation.OPERATION_CREATE, rec.getOperation());

        assertFalse(rec.getPayload().getBefore().hasStructuredData());
        assertFalse(rec.getPayload().getBefore().hasRawData());

        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        com.google.protobuf.Struct after = rec.getPayload().getAfter().getStructuredData();
    }
}
