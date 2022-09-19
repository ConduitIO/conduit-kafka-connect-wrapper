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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.conduit.grpc.Change;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
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
public class DebeziumToOpenCDC extends SourceRecordConverter implements Function<SourceRecord, Record.Builder> {
    private static final Map<String, Operation> DEBEZIUM_OPERATIONS = Map.of(
            "c", OPERATION_CREATE,
            "r", OPERATION_SNAPSHOT,
            "u", OPERATION_UPDATE,
            "d", OPERATION_DELETE
    );

    @Override
    public Record.Builder apply(SourceRecord record) {
        if (record == null) {
            return null;
        }
        if (record.valueSchema() == null || record.valueSchema().type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException("expected value schema to be STRUCT");
        }
        if (record.value() == null) {
            throw new IllegalArgumentException("record has no value");
        }

        // The transformer returns a builder, so that it's easier to set other fields on the same record.
        // We can return a record, but the caller would then need to transform it into a builder,
        // which might create needless copies of fields.
        return Record.newBuilder()
                .setKey(getKey(record))
                .setOperation(getOperation(record))
                .setPayload(getChangePayload(record))
                // we need nanoseconds here
                .putMetadata(OpenCdcMetadata.READ_AT, String.valueOf(System.currentTimeMillis() * 1_000_000))
                .putAllMetadata(getMetadata(record));
    }

    private Map<String, String> getMetadata(SourceRecord record) {
        Map<String, String> meta = new HashMap<>();
        Struct source = ((Struct) record.value()).getStruct("source");
        for (Field f : source.schema().fields()) {
            meta.put(
                "kafkaconnect.debezium.source." + f.name(),
                String.valueOf(source.get(f))
            );
        }
        return meta;
    }

    private Operation getOperation(SourceRecord record) {
        if (record.valueSchema() == null || record.valueSchema().type() != Schema.Type.STRUCT) {
            throw new IllegalArgumentException(
                    "expected a record with a struct payload, but got " + record.valueSchema()
            );
        }

        org.apache.kafka.connect.data.Struct struct = (org.apache.kafka.connect.data.Struct) record.value();
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
