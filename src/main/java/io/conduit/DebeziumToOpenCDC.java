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

import java.io.InvalidObjectException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.conduit.grpc.Change;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import static io.conduit.grpc.Operation.OPERATION_CREATE;
import static io.conduit.grpc.Operation.OPERATION_DELETE;
import static io.conduit.grpc.Operation.OPERATION_SNAPSHOT;
import static io.conduit.grpc.Operation.OPERATION_UPDATE;

/**
 * Transforms a Debezium record into an OpenCDC record.
 */
@AllArgsConstructor
public class DebeziumToOpenCDC extends SourceRecordConverter implements Function<SourceRecord, Record.Builder> {
    private static final Map<String, Operation> DEBEZIUM_OPERATIONS = Map.of(
        "c", OPERATION_CREATE,
        "r", OPERATION_SNAPSHOT,
        "u", OPERATION_UPDATE,
        "d", OPERATION_DELETE
    );

    private final boolean saveSchema;

    @Override
    public Record.Builder apply(SourceRecord rec) {
        if (rec == null) {
            return null;
        }
        if (rec.valueSchema() == null || rec.valueSchema().type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException("expected value schema to be STRUCT");
        }
        if (rec.value() == null) {
            throw new IllegalArgumentException("record has no value");
        }

        // The transformer returns a builder, so that it's easier to set other fields on the same record.
        // We can return a record, but the caller would then need to transform it into a builder,
        // which might create needless copies of fields.
        return Record.newBuilder()
            .setKey(getKey(rec))
            .setOperation(getOperation(rec))
            .setPayload(getChangePayload(rec))
            // we need nanoseconds here
            .putMetadata(OpenCdcMetadata.READ_AT, String.valueOf(System.currentTimeMillis() * 1_000_000))
            .putAllMetadata(getMetadata(rec));
    }

    private Map<String, String> getMetadata(SourceRecord rec) {
        Map<String, String> meta = new HashMap<>();
        addSourceMetadata(rec, meta);
        if (saveSchema) {
            addSchemaMetadata(meta, rec);
        }
        return meta;
    }

    @SneakyThrows
    private void addSchemaMetadata(Map<String, String> meta, SourceRecord rec) {
        // NB: The schema included here has exactly those fields
        // which are present in the record,
        // i.e. this is not the schema of the whole source table.
        ObjectNode valSchema = Utils.mapper.createObjectNode();
        Struct struct = ((Struct) rec.value()).getStruct("after");
        // fall back to the before field, if after is empty
        if (struct == null) {
            struct = ((Struct) rec.value()).getStruct("before");
        }
        // if before and after are empty, throw an exception
        if (struct == null) {
            throw new InvalidObjectException("after and before fields are empty for record key: " + rec.key());
        }

        for (Field f : struct.schema().fields()) {
            valSchema.put(f.name(), f.schema().type().toString());
        }

        meta.put("kafkaconnect.value.schema", Utils.mapper.writeValueAsString(valSchema));

        ObjectNode keySchema = Utils.mapper.createObjectNode();
        Struct key = ((Struct) rec.key());
        for (Field f : key.schema().fields()) {
            keySchema.put(f.name(), f.schema().type().toString());
        }
        meta.put("kafkaconnect.key.schema", Utils.mapper.writeValueAsString(keySchema));
    }

    private void addSourceMetadata(SourceRecord rec, Map<String, String> meta) {
        Struct source = ((Struct) rec.value()).getStruct("source");
        for (Field f : source.schema().fields()) {
            meta.put(
                "kafkaconnect.debezium.source." + f.name(),
                String.valueOf(source.get(f))
            );
        }
    }

    private Operation getOperation(SourceRecord rec) {
        if (rec.valueSchema() == null || rec.valueSchema().type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException(
                "expected a record with a struct payload, but got " + rec.valueSchema()
            );
        }

        org.apache.kafka.connect.data.Struct struct = (org.apache.kafka.connect.data.Struct) rec.value();
        String op = struct.getString("op");
        if (!DEBEZIUM_OPERATIONS.containsKey(op)) {
            throw new IllegalArgumentException("invalid Debezium operation: " + op);
        }
        return DEBEZIUM_OPERATIONS.get(op);
    }

    private Change getChangePayload(SourceRecord sourceRecord) {
        if (sourceRecord.valueSchema() != null) {
            return schemaValueToChange(sourceRecord.valueSchema(), sourceRecord.value());
        }
        throw new UnsupportedOperationException("payloads without schemas not supported yet");
    }

    @SneakyThrows
    private Change schemaValueToChange(Schema schema, Object value) {
        if (!(value instanceof org.apache.kafka.connect.data.Struct)) {
            throw new IllegalArgumentException("expected a Struct");
        }
        org.apache.kafka.connect.data.Struct struct = (org.apache.kafka.connect.data.Struct) value;

        Change.Builder builder = Change.newBuilder();
        org.apache.kafka.connect.data.Struct after = struct.getStruct("after");
        if (after != null) {
            builder.setAfter(schemaValueToData(after.schema(), after));
        }

        org.apache.kafka.connect.data.Struct before = struct.getStruct("before");
        if (before != null) {
            builder.setBefore(schemaValueToData(before.schema(), before));
        }
        return builder.build();
    }
}
