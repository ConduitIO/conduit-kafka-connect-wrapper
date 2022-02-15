package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.conduit.grpc.Record;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.conduit.Transformations.fromKafkaSource;
import static org.apache.kafka.connect.data.Schema.*;
import static org.junit.jupiter.api.Assertions.*;

public class TransformationsTest {
    private Schema testSchema;
    private Struct testValue;

    @BeforeEach
    public void setUp() {
        testSchema = new SchemaBuilder(Schema.Type.STRUCT)
                .field("id", INT32_SCHEMA)
                .field("name", STRING_SCHEMA)
                .field("company", STRING_SCHEMA)
                .field("verified", BOOLEAN_SCHEMA)
                .build();

        testValue = new Struct(testSchema);
        testValue.put("id", 998877);
        testValue.put("name", "test name");
        testValue.put("company", "test company");
        testValue.put("verified", true);
    }

    @Test
    public void testFromKafkaSource_Null() {
        assertNull(fromKafkaSource(null));
    }

    @SneakyThrows
    @Test
    public void testFromKafkaSource_WithValueSchema_NoKeySchema() {
        var sourceRecord = new SourceRecord(
                Map.of("test-partition", "test_table"),
                Map.of("test-offset", 123456L),
                "test-topic",
                2,
                testSchema,
                testValue
        );
        Record conduitRec = Transformations.fromKafkaSource(sourceRecord);
        assertNotNull(conduitRec);

        // verify payload
        var payload = conduitRec.getPayload().getStructuredData();
        assertMatch(testValue, payload);
        // verify key
        assertFalse(conduitRec.getKey().hasRawData());
        assertFalse(conduitRec.getKey().hasStructuredData());
        // verify position
        // it's a JSON object with the two keys mentioned below
        Map position = Utils.mapper.readValue(conduitRec.getPosition().toStringUtf8(), Map.class);
        assertEquals(sourceRecord.sourcePartition(), position.get("sourcePartition"));
        assertEquals(sourceRecord.sourceOffset(), position.get("sourceOffset"));
    }

    private void assertMatch(Struct expected, com.google.protobuf.Struct payload) {
        assertEquals(expected.get("id"), (int) payload.getFieldsOrThrow("id").getNumberValue());
        assertEquals(expected.get("name"), payload.getFieldsOrThrow("name").getStringValue());
        assertEquals(expected.get("company"), payload.getFieldsOrThrow("company").getStringValue());
        assertEquals(expected.get("verified"), payload.getFieldsOrThrow("verified").getBoolValue());
    }
}
