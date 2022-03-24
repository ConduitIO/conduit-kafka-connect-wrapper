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
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
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
    /**
     * Transforms a Kafka Connect {@link SourceRecord} into a Conduit record. Only the payload is set.
     */
    public static final Record.Builder fromKafkaSource(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            return null;
        }
        // NB: Aiven's JDBC source connector doesn't return keys, so we're skipping them here.
        // The transformer returns a builder, so that it's easier to set other fields on the same record.
        // We can return a record, but the caller would then need to transform it into a builder,
        // which might create needless copies of fields.
        return Record.newBuilder()
                .setPayload(getPayload(sourceRecord))
                .setCreatedAt(currentTimestamp());
    }

    private static Timestamp currentTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) (millis % 1000) * 1000)
                .build();
    }

    private static Data getPayload(SourceRecord sourceRecord) {
        if (sourceRecord.valueSchema() != null) {
            return schemaValueToData(sourceRecord.valueSchema(), sourceRecord.value());
        }
        return getRawData(sourceRecord);
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
        var outStruct = Struct.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(
                new String(bytes),
                outStruct
        );
        return Data.newBuilder()
                .setStructuredData(outStruct)
                .build();
    }

    private static Data getRawData(Object value) {
        throw new UnsupportedOperationException("records with raw data not supported yet");
    }

    /**
     * Transforms the input Conduit record into a Kafka connect data type
     * (if the record contains structured data or if a schema is provided).
     * Otherwise, returns the raw payload's byte data.
     */
    public static Object toConnectData(Record record, Schema schema) {
        if (record == null) {
            throw new IllegalArgumentException("record is null");
        }
        if (!record.hasPayload()) {
            throw new IllegalArgumentException("record has no payload");
        }

        if (record.getPayload().hasStructuredData()) {
            return Transformations.parseStructured(record, schema);
        } else {
            return Transformations.parseRaw(record, schema);
        }
    }

    @SneakyThrows
    private static Object parseStructured(Record record, Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("cannot parse struct without schema");
        }
        // Struct -> JSON string -> bytes
        // todo this is a non-optimal way to do it
        // however, for the first version of the plugin we're looking for making it work first.
        byte[] bytes = JsonFormat.printer()
                .print(record.getPayload().getStructuredData())
                .getBytes(StandardCharsets.UTF_8);
        return jsonToStruct(bytes, schema);
    }

    @SneakyThrows
    private static Object parseRaw(Record record, Schema schema) {
        byte[] content = record.getPayload().getRawData().toByteArray();
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
        // for each record, we're creating a new JSON object
        // and copying data into it.
        // something as simple as concatenating strings could work.
        ObjectNode json = Utils.mapper.createObjectNode();
        json.set("schema", Utils.jsonConv.asJsonSchema(schema));
        json.set("payload", Utils.mapper.readTree(content));

        byte[] bytes = Utils.mapper.writeValueAsBytes(json);
        // topic arg unused in the connect-json library
        return Utils.jsonConv.toConnectData("", bytes).value();
    }
}
