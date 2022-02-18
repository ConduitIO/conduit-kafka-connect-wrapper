package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Transformations {
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
