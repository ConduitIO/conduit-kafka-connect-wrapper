package io.conduit;

import java.nio.charset.StandardCharsets;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Record;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;

/**
 * A {@link SchemaProvider} implementation which first infers the Avro schema,
 * and then converts it to a Kafka Connect schema.
 */
public class DefaultSchemaProvider implements SchemaProvider {
    private static final AvroData AVRO_DATA = new AvroData(new AvroDataConfig(new HashMap<>()));

    @SneakyThrows
    @Override
    public Schema provide(Record record, String name) {
        byte[] bytes;
        if (record.getPayload().hasStructuredData()) {
            bytes = JsonFormat.printer()
                    .print(record.getPayload().getStructuredData())
                    .getBytes(StandardCharsets.UTF_8);
        } else {
            bytes = record.getPayload().getRawData().toStringUtf8().getBytes(StandardCharsets.UTF_8);
        }
        return provide(Utils.mapper.readTree(bytes), name);
    }

    private Schema provide(JsonNode json, String name) {
        return AVRO_DATA.toConnectSchema(
                inferAvroSchema(json, name)
        );
    }

    private org.apache.avro.Schema inferAvroSchema(JsonNode node, String name) {
        return visit(node, new JsonSchemaVisitor(name));
    }

    private <T> T visit(JsonNode node, JsonTreeVisitor<T> visitor) {
        switch (node.getNodeType()) {
            case OBJECT:
                Preconditions.checkArgument(node instanceof ObjectNode,
                        "Expected instance of ObjectNode: " + node);

                // use LinkedHashMap to preserve field order
                Map<String, T> fields = Maps.newLinkedHashMap();

                Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
                while (iter.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iter.next();

                    visitor.recordLevels.push(entry.getKey());
                    fields.put(entry.getKey(), visit(entry.getValue(), visitor));
                    visitor.recordLevels.pop();
                }

                return visitor.object((ObjectNode) node, fields);

            case ARRAY:
                Preconditions.checkArgument(node instanceof ArrayNode,
                        "Expected instance of ArrayNode: " + node);

                List<T> elements = Lists.newArrayListWithExpectedSize(node.size());

                for (JsonNode element : node) {
                    elements.add(visit(element, visitor));
                }

                return visitor.array((ArrayNode) node, elements);

            case BINARY:
                Preconditions.checkArgument(node instanceof BinaryNode,
                        "Expected instance of BinaryNode: " + node);
                return visitor.binary((BinaryNode) node);

            case STRING:
                Preconditions.checkArgument(node instanceof TextNode,
                        "Expected instance of TextNode: " + node);

                return visitor.text((TextNode) node);

            case NUMBER:
                Preconditions.checkArgument(node instanceof NumericNode,
                        "Expected instance of NumericNode: " + node);

                return visitor.number((NumericNode) node);

            case BOOLEAN:
                Preconditions.checkArgument(node instanceof BooleanNode,
                        "Expected instance of BooleanNode: " + node);

                return visitor.bool((BooleanNode) node);

            case MISSING:
                Preconditions.checkArgument(node instanceof MissingNode,
                        "Expected instance of MissingNode: " + node);

                return visitor.missing((MissingNode) node);

            case NULL:
                Preconditions.checkArgument(node instanceof NullNode,
                        "Expected instance of NullNode: " + node);

                return visitor.nullNode((NullNode) node);

            default:
                throw new IllegalArgumentException(
                        "Unknown node type: " + node.getNodeType() + ": " + node);
        }
    }

    public abstract static class JsonTreeVisitor<T> {
        protected LinkedList<String> recordLevels = Lists.newLinkedList();

        public T object(ObjectNode object, Map<String, T> fields) {
            return null;
        }

        public T array(ArrayNode array, List<T> elements) {
            return null;
        }

        public T binary(BinaryNode binary) {
            return null;
        }

        public T text(TextNode text) {
            return null;
        }

        public T number(NumericNode number) {
            return null;
        }

        public T bool(BooleanNode bool) {
            return null;
        }

        public T missing(MissingNode missing) {
            return null;
        }

        public T nullNode(NullNode nullNode) {
            return null;
        }
    }

    private static class JsonSchemaVisitor extends JsonTreeVisitor<org.apache.avro.Schema> {

        private static final Joiner DOT = Joiner.on('.');
        private final String name;
        private boolean objectsToRecords = true;

        public JsonSchemaVisitor(String name) {
            this.name = name;
        }

        public JsonSchemaVisitor useMaps() {
            this.objectsToRecords = false;
            return this;
        }

        @Override
        public org.apache.avro.Schema object(ObjectNode object, Map<String, org.apache.avro.Schema> fields) {
            if (objectsToRecords || recordLevels.size() < 1) {
                List<org.apache.avro.Schema.Field> recordFields = Lists.newArrayListWithExpectedSize(
                        fields.size());

                for (Map.Entry<String, org.apache.avro.Schema> entry : fields.entrySet()) {
                    recordFields.add(new org.apache.avro.Schema.Field(
                            entry.getKey(), entry.getValue(),
                            "Type inferred from '" + object.get(entry.getKey()) + "'",
                            null));
                }

                org.apache.avro.Schema recordSchema;
                if (recordLevels.size() < 1) {
                    recordSchema = org.apache.avro.Schema.createRecord(name, null, null, false);
                } else {
                    recordSchema = org.apache.avro.Schema.createRecord(
                            DOT.join(recordLevels), null, null, false);
                }

                recordSchema.setFields(recordFields);

                return recordSchema;

            } else {
                // translate to a map; use LinkedHashSet to preserve schema order
                switch (fields.size()) {
                    case 0:
                        return org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
                    case 1:
                        return org.apache.avro.Schema.createMap(Iterables.getOnlyElement(fields.values()));
                    default:
                        return org.apache.avro.Schema.createMap(SchemaUtil.mergeOrUnion(fields.values()));
                }
            }
        }

        @Override
        public org.apache.avro.Schema array(ArrayNode ignored, List<org.apache.avro.Schema> elementSchemas) {
            // use LinkedHashSet to preserve schema order
            switch (elementSchemas.size()) {
                case 0:
                    return org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));
                case 1:
                    return org.apache.avro.Schema.createArray(Iterables.getOnlyElement(elementSchemas));
                default:
                    return org.apache.avro.Schema.createArray(SchemaUtil.mergeOrUnion(elementSchemas));
            }
        }

        @Override
        public org.apache.avro.Schema binary(BinaryNode ignored) {
            return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
        }

        @Override
        public org.apache.avro.Schema text(TextNode ignored) {
            return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        }

        @Override
        public org.apache.avro.Schema number(NumericNode number) {
            if (number.isInt()) {
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
            } else if (number.isLong()) {
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
            } else if (number.isFloat()) {
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT);
            } else if (number.isDouble()) {
                return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
            } else {
                throw new UnsupportedOperationException(
                        number.getClass().getName() + " is not supported");
            }
        }

        @Override
        public org.apache.avro.Schema bool(BooleanNode ignored) {
            return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN);
        }

        @Override
        public org.apache.avro.Schema nullNode(NullNode ignored) {
            return org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);
        }

        @Override
        public org.apache.avro.Schema missing(MissingNode ignored) {
            throw new UnsupportedOperationException("MissingNode is not supported.");
        }
    }
}
