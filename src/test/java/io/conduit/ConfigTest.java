package io.conduit;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigTest {
    @Test
    public void testFromMap() {
        Map<String, String> map = Map.of(
                "wrapper.connector.class", "test-connector-class",
                "wrapper.logs.root.level", "INFO",
                "wrapper.logs.io.foo.bar.level", "DEBUG",
                "kafka.connector.param1", "test-value-1",
                "kafka.connector.param2", "test-value-2"
        );
        Config actual = Config.fromMap(map);
        assertEquals(
            new Config(
                map.get("wrapper.connector.class"),
                Map.of(
                    "root", "INFO",
                    "io.foo.bar", "DEBUG"
                ),
                Map.of(
                    "kafka.connector.param1", "test-value-1",
                    "kafka.connector.param2", "test-value-2"
                )
            ),
            actual
        );
    }
}
