package io.conduit;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.conduit.grpc.Record;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@Slf4j
@AllArgsConstructor
public class RawDataSchemaProvider implements SchemaProvider {
    private final String name;
    private final Schema overrides;

    @Override
    public Schema provide(Record record) {
        if (!record.hasPayload()) {
            return null;
        }
        if (!record.getPayload().hasRawData()) {
            throw new IllegalArgumentException("Record has no raw data.");
        }
        JsonNode json = parseJson(record.getPayload().getRawData().toByteArray());
        if (json == null) {
            return Schema.OPTIONAL_BYTES_SCHEMA;
        }
        return schemaForJson(json);
    }

    private Schema schemaForJson(JsonNode json) {
        switch (json.getNodeType()) {
            case OBJECT:
                return schemaForObject(json);
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case ARRAY:
                return schemaForArray(json);
            case NUMBER:
                return schemaForNumber(json);
            default:
                return null;
        }
    }

    private Schema schemaForArray(JsonNode json) {
        var array = (ArrayNode) json;
        // if there are no values, we can't determine what's the array element type.
        if (array == null || array.size() == 0) {
            return null;
        }
        return SchemaBuilder
                .array(schemaForJson(array.get(0)))
                .optional()
                .build();
    }

    private Schema schemaForObject(JsonNode json) {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT)
                .name(name)
                .optional();

        json.fields().forEachRemaining(entry -> {
            if (overrides != null && overrides.field(entry.getKey()) != null) {
                builder.field(entry.getKey(), overrides.field(entry.getKey()).schema());
            } else {
                Schema fieldSchema = schemaForJson(entry.getValue());
                if (fieldSchema != null) {
                    builder.field(entry.getKey(), fieldSchema);
                }
            }
        });

        return builder.build();
    }

    private Schema schemaForNumber(JsonNode json) {
        if (json.isShort()) {
            return Schema.OPTIONAL_INT16_SCHEMA;
        }
        if (json.isInt()) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        }
        if (json.isLong()) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        }
        if (json.isFloat()) {
            return Schema.OPTIONAL_FLOAT32_SCHEMA;
        }
        if (json.isDouble()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }
        return null;
    }

    private JsonNode parseJson(byte[] bytes) {
        try {
            return Utils.mapper.readTree(bytes);
        } catch (IOException e) {
            log.info("Couldn't parse as JSON.", e);
            return null;
        }
    }
}