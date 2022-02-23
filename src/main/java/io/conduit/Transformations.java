package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.conduit.Utils.jsonConvSchemaless;
import static java.util.Collections.emptyMap;

public class Transformations {
    public static final Record fromKafkaSource(SourceRecord sourceRecord) {
        if (sourceRecord == null) {
            return null;
        }
        // NB: Aiven's JDBC source connector doesn't return keys, so we're skipping them here.
        return Record.newBuilder()
                .setPayload(getPayload(sourceRecord))
                .setPosition(getPosition(sourceRecord))
                .build();
    }

    @SneakyThrows
    private static ByteString getPosition(SourceRecord sourceRecord) {
        Map<String, Object> position = new HashMap<>();
        // We assume all records come from a single source partition (e.g. table in a DB)
        // Support for multiple partitions will be added separately.
        position.put("sourcePartition", sourceRecord.sourcePartition());
        position.put("sourceOffset", sourceRecord.sourceOffset());
        String json = Utils.mapper.writeValueAsString(position);
        return ByteString.copyFromUtf8(json);
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

    @SneakyThrows
    public static Map<String, Map<String, Object>> parsePosition(String position) {
        if (Utils.isEmpty(position)) {
            return emptyMap();
        }
        return Utils.mapper.readValue(position, Map.class);
    }

    public static Object toStruct(Record record, ObjectNode schemaJson) {
        if (record == null) {
            throw new IllegalArgumentException("record is null");
        }
        if (!record.hasPayload()) {
            throw new IllegalArgumentException("record has no payload");
        }

        if (record.getPayload().hasStructuredData()) {
            return Transformations.parseStructured(record, schemaJson);
        } else {
            return Transformations.parseRawJson(record, schemaJson);
        }
    }

    @SneakyThrows
    public static Object parseStructured(Record record, ObjectNode schemaJson) {
        if (schemaJson == null) {
            throw new IllegalArgumentException("cannot parse struct without schema");
        }
        // Struct -> JSON string -> bytes
        // todo this is a non-optimal way to do it
        // however, for the first version of the plugin we're looking for making it work first.
        byte[] bytes = JsonFormat.printer()
                .print(record.getPayload().getStructuredData())
                .getBytes(StandardCharsets.UTF_8);
        return bytesToStruct(bytes, schemaJson);
    }

    @SneakyThrows
    public static Object parseRawJson(Record record, ObjectNode schemaJson) {
        byte[] content = record.getPayload().getRawData().toByteArray();
        if (schemaJson == null) {
            return content;
        }
        return bytesToStruct(content, schemaJson);
    }

    private static Object bytesToStruct(byte[] content, ObjectNode schemaJson) throws IOException {
        // todo optimize memory usage here
        // for each record, we're creating a new JSON object
        // and copying data into it.
        // something as simple as concatenating strings could work.
        JsonNode payloadJson = Utils.mapper.readTree(content);

        ObjectNode json = Utils.mapper.createObjectNode();
        json.set("schema", schemaJson);
        json.set("payload", payloadJson);

        byte[] bytes = Utils.mapper.writeValueAsBytes(json);
        // topic arg unused in the connect-json library
        return Utils.jsonConv.toConnectData("", bytes).value();
    }
}
