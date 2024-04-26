package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigTest {
    @Test
    void testFromMap() {
        Map<String, String> map = Map.of(
            "wrapper.connector.class", "test-connector-class",
            "wrapper.log.level", "INFO",
            "wrapper.debezium.schema.save", "true",
            "kafka.connector.param1", "test-value-1",
            "kafka.connector.param2", "test-value-2"
        );
        Config actual = Config.fromMap(map);
        assertEquals("test-connector-class", actual.getConnectorClass());
        assertEquals("INFO", actual.getLogLevel());
        assertTrue(actual.isSaveSchema());
        assertEquals(
            Map.of(
                "connector.class", "test-connector-class",
                "kafka.connector.param1", "test-value-1",
                "kafka.connector.param2", "test-value-2"
            ),
            actual.getKafkaConnectorCfg()
        );
    }
}
