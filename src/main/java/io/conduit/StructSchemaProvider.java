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

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.conduit.grpc.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A {@link SchemaProvider} implementation providing schemas for {@link Record}s which have structured payloads.
 */
@AllArgsConstructor
public class StructSchemaProvider implements SchemaProvider {
    private final String name;
    private final Schema overrides;

    // todo configure usage of optional values
    @Override
    public Schema provide(Record record) {
        if (!record.hasPayload()) {
            return null;
        }
        if (!record.getPayload().hasStructuredData()) {
            throw new IllegalArgumentException("Record has no structured payload.");
        }
        return schemaForStruct(record.getPayload().getStructuredData());
    }

    private Schema schemaForStruct(Struct struct) {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT)
                .name(name)
                .optional();

        struct.getFieldsMap().forEach((key, value) -> {
            if (overrides != null && overrides.field(key) != null) {
                builder.field(key, overrides.field(key).schema());
            } else {
                Schema fieldSchema = schemaForValue(value);
                if (fieldSchema != null) {
                    builder.field(key, fieldSchema);
                }
            }
        });

        return builder.build();
    }

    private Schema schemaForValue(Value value) {
        if (value.hasBoolValue()) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        }
        if (value.hasNumberValue()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }
        if (value.hasStringValue()) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
        if (value.hasStructValue()) {
            return schemaForStruct(value.getStructValue());
        }
        if (value.hasListValue()) {
            return schemaForList(value.getListValue());
        }
        return null;
    }

    private Schema schemaForList(ListValue list) {
        // if there are no values, we can't determine what's the array element type.
        if (list == null || list.getValuesCount() == 0) {
            return null;
        }
        // todo in theory at least, list values can be of different types
        // handle that case here
        return SchemaBuilder
                .array(schemaForValue(list.getValues(0)))
                .optional()
                .build();
    }
}
