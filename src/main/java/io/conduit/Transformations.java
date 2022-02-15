package io.conduit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
        Data.Builder builder = Data.newBuilder();
        if (value == null) {
            return builder.build();
        }
        return builder
                .setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .build();
    }

    @SneakyThrows
    public static Map<String, Map<String, Object>> parsePosition(String position) {
        if (Utils.isEmpty(position)) {
            return emptyMap();
        }
        return Utils.mapper.readValue(position, Map.class);
    }
}
