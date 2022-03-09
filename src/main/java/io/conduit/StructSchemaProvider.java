package io.conduit;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.conduit.grpc.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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
        if (record.getPayload().hasStructuredData()) {
            return provideForStruct(record.getPayload().getStructuredData());
        }
        return null;
    }

    private Schema provideForStruct(Struct struct) {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT)
                .name(name)
                .optional();

        struct.getFieldsMap().forEach((key, value) -> {
            if (overrides != null && overrides.field(key) != null) {
                builder.field(key, overrides.field(key).schema());
            } else {
                Schema fieldSchema = provideForValue(value);
                if (fieldSchema != null) {
                    builder.field(key, fieldSchema);
                }
            }
        });

        return builder.build();
    }

    private Schema provideForValue(Value value) {
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
            return provideForStruct(value.getStructValue());
        }
        if (value.hasListValue()) {
            return provideForList(value.getListValue());
        }
        return null;
    }

    private Schema provideForList(ListValue list) {
        // if there are no values, we can't determine what's the array element type.
        if (list == null || list.getValuesCount() == 0) {
            return null;
        }
        // todo in theory at least, list values can be of different types
        // handle that case here
        return SchemaBuilder
                .array(provideForValue(list.getValues(0)))
                .optional()
                .build();
    }
}
