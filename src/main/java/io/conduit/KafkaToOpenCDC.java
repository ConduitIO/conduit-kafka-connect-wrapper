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

import java.util.function.Function;

import io.conduit.grpc.Change;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Transforms a Kafka Connect {@link SourceRecord} into a Conduit record. Only the payload is set.
 */
public class KafkaToOpenCDC extends SourceRecordConverter implements Function<SourceRecord, Record.Builder> {
    @Override
    public Record.Builder apply(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            return null;
        }
        // The transformer returns a builder, so that it's easier to set other fields on the same record.
        // We can return a record, but the caller would then need to transform it into a builder,
        // which might create needless copies of fields.
        return Record.newBuilder()
                .setKey(getKey(sourceRecord))
                .setPayload(getPayload(sourceRecord))
                .setOperation(Operation.OPERATION_CREATE)
                // we need nanoseconds here
                .putMetadata(OpenCdcMetadata.READ_AT, String.valueOf(System.currentTimeMillis() * 1_000_000));
    }

    private Change getPayload(SourceRecord sourceRecord) {
        if (sourceRecord.valueSchema() != null) {
            return schemaValueToAfter(sourceRecord.valueSchema(), sourceRecord.value());
        }
        throw new UnsupportedOperationException("payloads without schemas not supported yet");
    }

    @SneakyThrows
    private Change schemaValueToAfter(Schema schema, Object value) {
        return Change.newBuilder()
                .setAfter(schemaValueToData(schema, value))
                .build();
    }
}
