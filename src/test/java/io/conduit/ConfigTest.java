package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigTest {
    @Test
    public void testFromMap() {
        Map<String, String> map = Map.of(
                "wrapper.connector.class", "test-connector-class",
                "wrapper.connector.name", "test-connector-name",
                "wrapper.pipeline.id", "test-pipeline-id",
                "kafka.connector.param1", "test-value-1",
                "kafka.connector.param2", "test-value-2"
        );
        Config cfg = Config.fromMap(map);
        assertEquals(map.get("wrapper.connector.class"), cfg.getConnectorClass());
        assertEquals(map.get("wrapper.connector.name"), cfg.getConnectorName());
        assertEquals(map.get("wrapper.pipeline.id"), cfg.getPipelineId());
        assertEquals(
                Map.of(
                        "kafka.connector.param1", "test-value-1",
                        "kafka.connector.param2", "test-value-2"
                ),
                cfg.getKafkaConnectorCfg()
        );
    }
}