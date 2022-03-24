package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigTest {
    @Test
    public void testFromMap() {
        Map<String, String> map = Map.of(
                "wrapper.task.class", "test-task-class",
                "kafka.connector.param1", "test-value-1",
                "kafka.connector.param2", "test-value-2"
        );
        Config cfg = Config.fromMap(map);
        assertEquals(map.get("wrapper.task.class"), cfg.getTaskClass());
        assertEquals(
                Map.of(
                        "kafka.connector.param1", "test-value-1",
                        "kafka.connector.param2", "test-value-2"
                ),
                cfg.getKafkaConnectorCfg()
        );
    }
}