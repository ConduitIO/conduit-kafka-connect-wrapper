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
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import static io.conduit.Utils.jsonConvSchemaless;

/**
 * A class holding common transformations between Conduit and Kafka connect data types.
 */
public class Transformations {

    private Transformations() {

    }

    /**
     * Transforms a Kafka Connect {@link SourceRecord} into a Conduit record. Only the payload is set.
     */
    public static final Record.Builder fromKafkaSource(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            return null;
        }
        // The transformer returns a builder, so that it's easier to set other fields on the same record.
        // We can return a record, but the caller would then need to transform it into a builder,
        // which might create needless copies of fields.
        return Record.newBuilder()
                .setKey(getKey(sourceRecord))
                .setPayload(getPayload(sourceRecord))
                // we need nanoseconds here
                .putMetadata(OpenCdcMetadata.CREATED_AT, String.valueOf(System.currentTimeMillis() * 1_000_000));
    }

    private static Change getPayload(SourceRecord sourceRecord) {
        if (sourceRecord.valueSchema() != null) {
            return schemaValueToChange(sourceRecord.valueSchema(), sourceRecord.value());
        }
        throw new UnsupportedOperationException("payloads without schemas not supported yet");
    }

    private static Data getKey(SourceRecord sourceRecord) {
        if (sourceRecord.key() == null) {
            return Data.newBuilder().build();
        }
        if (sourceRecord.keySchema() != null) {
            return schemaValueToData(sourceRecord.keySchema(), sourceRecord.key());
        }
        throw new UnsupportedOperationException("keys without schemas not supported yet");
    }

    @SneakyThrows
    private static Change schemaValueToChange(Schema schema, Object value) {
        return Change.newBuilder()
                .setAfter(schemaValueToData(schema, value))
                .build();
    }

    @SneakyThrows
    private static Data schemaValueToData(Schema schema, Object value) {
        byte[] bytes = jsonConvSchemaless.fromConnectData("", schema, value);
        if (schema.type() != Schema.Type.STRUCT) {
            return Data.newBuilder()
                    .setRawData(ByteString.copyFrom(bytes))
                    .build();
        }
        // todo try improving performance
        // Internally, Kafka's JsonConverter transforms the input value into a JsonNode, then into a bytes array.
        // Then, we create a string from the bytes array, and let the Protobuf Java library parse the struct.
        // This lets us quickly get the correct struct, without having to handle all the possible types,
        // but is probably not as efficient as it can be.
        // See: https://github.com/ConduitIO/conduit-kafka-connect-wrapper/issues/60
        var outStruct = Struct.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(
                new String(bytes),
                outStruct
        );
        return Data.newBuilder()
                .setStructuredData(outStruct)
                .build();
    }

    /**
     * Transforms the input Conduit record into a Kafka connect data type
     * (if the record contains structured data or if a schema is provided).
     * Otherwise, returns the raw payload's byte data.
     */
    public static Object toConnectData(Record rec, Schema schema) {
        if (rec == null) {
            throw new IllegalArgumentException("record is null");
        }
        if (!rec.hasPayload() || !rec.getPayload().hasAfter()) {
            throw new IllegalArgumentException("record has no payload or has no after data");
        }

        if (rec.getPayload().getAfter().hasStructuredData()) {
            return Transformations.parseStructured(rec, schema);
        } else {
            return Transformations.parseRaw(rec, schema);
        }
    }

    @SneakyThrows
    private static Object parseStructured(Record rec, Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("cannot parse struct without schema");
        }
        // Struct -> JSON string -> bytes
        // todo this is a non-optimal way to do it
        // however, for the first version of the plugin we're looking for making it work first.
        // See: https://github.com/ConduitIO/conduit-kafka-connect-wrapper/issues/58
        byte[] bytes = JsonFormat.printer()
                .print(rec.getPayload().getAfter().getStructuredData())
                .getBytes(StandardCharsets.UTF_8);
        return jsonToStruct(bytes, schema);
    }

    @SneakyThrows
    private static Object parseRaw(Record rec, Schema schema) {
        byte[] content = rec.getPayload().getAfter().getRawData().toByteArray();
        if (schema == null) {
            return content;
        }
        if (Schema.Type.STRING.equals(schema.type())) {
            return new String(content, StandardCharsets.UTF_8);
        }
        if (!Schema.Type.STRUCT.equals(schema.type())) {
            return content;
        }
        return jsonToStruct(content, schema);
    }

    private static Object jsonToStruct(byte[] content, Schema schema) throws IOException {
        // todo optimize memory usage here
        // See: https://github.com/ConduitIO/conduit-kafka-connect-wrapper/issues/58
        ObjectNode json = Utils.mapper.createObjectNode();
        json.set("schema", Utils.jsonConv.asJsonSchema(schema));
        json.set("payload", Utils.mapper.readTree(content));

        byte[] bytes = Utils.mapper.writeValueAsBytes(json);
        // topic arg unused in the connect-json library
        return Utils.jsonConv.toConnectData("", bytes).value();
    }
}
