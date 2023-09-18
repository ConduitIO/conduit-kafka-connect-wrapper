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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.google.protobuf.Value;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaToOpenCDCTest {
    private KafkaToOpenCDC underTest;
    private Schema valueSchema;
    private Schema keySchema;
    private Struct testValue;

    @BeforeEach
    public void setUp() {
        underTest = new KafkaToOpenCDC();

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
    }

    @Test
    public void testFromKafkaSource_Null() {
        assertNull(underTest.apply(null));
    }

    @SneakyThrows
    @Test
    public void testFromKafkaSource_WithValueSchema_NoKey() {
        var sourceRecord = new SourceRecord(
            Map.of("test-partition", "test_table"),
            Map.of("test-offset", 123456L),
            "test-topic",
            2,
            valueSchema,
            testValue
        );
        Record.Builder conduitRec = underTest.apply(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getAfter().getStructuredData();
        assertMatch(testValue, payload);
        // assert timestamp is within last second
        long createdAt = Long.parseLong(conduitRec.getMetadataOrThrow(OpenCdcMetadata.READ_AT));
        assertTrue(
            createdAt / 1_000_000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertFalse(conduitRec.getKey().hasStructuredData());
    }

    @SneakyThrows
    @Test
    public void testFromKafkaSource_WithValueSchema_NoKeySchema() {
        var sourceRecord = new SourceRecord(
            Map.of("test-partition", "test_table"),
            Map.of("test-offset", 123456L),
            "test-topic",
            2,
            null,
            "test-key",
            valueSchema,
            testValue
        );
        var e = assertThrows(
            UnsupportedOperationException.class,
            () -> underTest.apply(sourceRecord)
        );
        assertEquals("keys without schemas not supported yet", e.getMessage());
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
        Record.Builder conduitRec = underTest.apply(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getAfter().getStructuredData();
        assertMatch(testValue, payload);
        // assert timestamp is within last second
        long createdAt = Long.parseLong(conduitRec.getMetadataOrThrow(OpenCdcMetadata.READ_AT));
        assertTrue(
            createdAt / 1_000_000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertTrue(conduitRec.getKey().hasStructuredData());
        com.google.protobuf.Struct key = conduitRec.getKey().getStructuredData();
        assertEquals(123, key.getFieldsOrThrow("id").getNumberValue());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("nonStructValuesTestCases")
    void nonStructValues(Schema inSchema, Object inValue, Data expectedPayload) {
        var sourceRecord = new SourceRecord(
            Map.of("test-partition", "test_table"),
            Map.of("test-offset", 123456L),
            "test-topic",
            2,
            keySchema,
            new Struct(keySchema).put("id", 123),
            inSchema,
            inValue
        );
        Record.Builder conduitRec = underTest.apply(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        assertEquals(expectedPayload, conduitRec.getPayload().getAfter());
        // assert timestamp is within last second
        long createdAt = Long.parseLong(conduitRec.getMetadataOrThrow(OpenCdcMetadata.READ_AT));
        assertTrue(
            createdAt / 1_000_000 > System.currentTimeMillis() - 1000
        );
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertTrue(conduitRec.getKey().hasStructuredData());
        com.google.protobuf.Struct key = conduitRec.getKey().getStructuredData();
        assertEquals(123, key.getFieldsOrThrow("id").getNumberValue());
    }

    static Stream<Arguments> nonStructValuesTestCases() {
        return Stream.of(
            Arguments.of(
                Schema.STRING_SCHEMA,
                "test payload",
                Data.newBuilder()
                    .setRawData(ByteString.copyFromUtf8("test payload"))
                    .build()
            ),
            Arguments.of(
                Schema.INT64_SCHEMA,
                123456789L,
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(ByteBuffer.allocate(8).putLong(123456789).array()))
                    .build()
            ),
            Arguments.of(
                Schema.INT32_SCHEMA,
                123456789,
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(ByteBuffer.allocate(4).putInt(123456789).array()))
                    .build()
            ),
            Arguments.of(
                Schema.FLOAT32_SCHEMA,
                123.456f,
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(ByteBuffer.allocate(4).putFloat(123.456f).array()))
                    .build()
            ),
            Arguments.of(
                Schema.FLOAT64_SCHEMA,
                123.456,
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(ByteBuffer.allocate(8).putDouble(123.456).array()))
                    .build()
            ),
            Arguments.of(
                Schema.BOOLEAN_SCHEMA,
                true,
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(new byte[]{1}))
                    .build()
            ),
            Arguments.of(
                Schema.BYTES_SCHEMA,
                new byte[]{1, 2, 3, 4, 5},
                Data.newBuilder()
                    .setRawData(ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5}))
                    .build()
            )
        );
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


}
