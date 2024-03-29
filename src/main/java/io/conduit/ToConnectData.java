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
import java.util.function.BiFunction;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;

/**
 * Transforms the input Conduit record into a Kafka connect data type
 * (if the record contains structured data or if a schema is provided).
 * Otherwise, returns the raw payload's byte data.
 */
public class ToConnectData implements BiFunction<Record, Schema, Object> {
    @Override
    public Object apply(Record rec, Schema schema) {
        if (rec == null) {
            throw new IllegalArgumentException("record is null");
        }
        if (!rec.hasPayload() || !rec.getPayload().hasAfter()) {
            throw new IllegalArgumentException("record has no payload or has no after data");
        }

        if (rec.getPayload().getAfter().hasStructuredData()) {
            return parseStructured(rec, schema);
        } else {
            return parseRaw(rec, schema);
        }
    }

    @SneakyThrows
    private Object parseStructured(Record rec, Schema schema) {
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
    private Object parseRaw(Record rec, Schema schema) {
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

    private Object jsonToStruct(byte[] content, Schema schema) throws IOException {
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
