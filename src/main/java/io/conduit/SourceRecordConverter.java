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

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Data;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import static io.conduit.Utils.jsonConvSchemaless;

abstract class SourceRecordConverter {
    protected Data getKey(SourceRecord sourceRecord) {
        if (sourceRecord.key() == null) {
            return Data.newBuilder().build();
        }
        if (sourceRecord.keySchema() != null) {
            return schemaValueToData(sourceRecord.keySchema(), sourceRecord.key());
        }
        throw new UnsupportedOperationException("keys without schemas not supported yet");
    }

    @SneakyThrows
    protected Data schemaValueToData(Schema schema, Object value) {
        Data data;
        switch (schema.type()) {
            case BYTES -> data = Data.newBuilder()
                .setRawData(ByteString.copyFrom((byte[]) value))
                .build();
            case STRING -> data = Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8((String) value))
                .build();
            case INT8, INT16, INT32 ->
                data = Data.newBuilder()
                .setRawData(
                    ByteString.copyFrom(new IntegerSerializer().serialize("", (Integer) value))
                ).build();
            case INT64 -> data = Data.newBuilder()
                .setRawData(
                    ByteString.copyFrom(new LongSerializer().serialize("", (Long) value))
                ).build();
            case FLOAT32 -> data = Data.newBuilder()
                .setRawData(ByteString.copyFrom(new FloatSerializer().serialize("", (Float) value)))
                .build();
            case FLOAT64 -> data = Data.newBuilder()
                .setRawData(ByteString.copyFrom(new DoubleSerializer().serialize("", (Double) value)))
                .build();
            case BOOLEAN -> data = Data.newBuilder()
                .setRawData(ByteString.copyFrom(new BooleanSerializer().serialize("", (Boolean) value)))
                .build();
            case STRUCT -> {
                var bytes = jsonConvSchemaless.fromConnectData("", schema, value);
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
                data = Data.newBuilder()
                    .setStructuredData(outStruct)
                    .build();
            }
            default -> throw new IllegalArgumentException("unrecognized schema: " + schema);
        }

        return data;
    }
}
