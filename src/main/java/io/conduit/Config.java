/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import static io.conduit.Utils.mapper;

/**
 * Represents a basis for source or destination configurations.
 * Contains the configuration for the Conduit connector and the underlying Kafka connector.
 * Conduit connector configuration parameters are prefixed with <code>wrapper.</code>.
 */
@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Config {
    public static final String WRAPPER_PREFIX = "wrapper.";
    public static final String WRAPPER_LOGS_PREFIX = "wrapper.logs.";

    @JsonProperty("connector.class")
    private String connectorClass;
    @JsonProperty("logs")
    // Maps logger name to level, e.g. "io.conduit" -> "DEBUG"
    private Map<String, String> logsConfig;
    private Map<String, String> kafkaConnectorCfg = new HashMap<>();

    /**
     * Creates a new <code>Config</code> instance from a map with configuration parameters.
     */
    public static Config fromMap(Map<String, String> map) {
        if (Utils.isEmpty(map)) {
            return new Config();
        }
        return fromMap(map, Config.class);
    }

    protected static <T extends Config> T fromMap(Map<String, String> map, Class<T> clazz) {
        Map<String, String> wrapperMap = new HashMap<>();
        Map<String, String> logsCfgMap = new HashMap<>();
        Map<String, String> connectorMap = new HashMap<>();

        map.forEach((k, v) -> {
            if (k.startsWith(WRAPPER_LOGS_PREFIX)) {
                logsCfgMap.put(k.replaceFirst(WRAPPER_LOGS_PREFIX, ""), v);
            } else if (k.startsWith(WRAPPER_PREFIX)) {
                wrapperMap.put(k.replaceFirst(WRAPPER_PREFIX, ""), v);
            } else {
                connectorMap.put(k, v);
            }
        });

        T cfg = mapper.convertValue(wrapperMap, clazz);
        cfg.setKafkaConnectorCfg(connectorMap);
        cfg.setLogsConfig(parseLogsConfig(logsCfgMap));
        return cfg;
    }

    private static Map<String, String> parseLogsConfig(Map<String, String> input) {
        Map<String, String> parsed = new HashMap<>();
        for (Map.Entry<String, String> e : input.entrySet()) {
            if (!e.getKey().endsWith(".level")) {
                throw new IllegalArgumentException(
                    "invalid logging configuration: key needs to end with '.level', input: " + e.getKey()
                );
            }

            // remove last 6 chars, i.e. the .level suffix
            String loggerName = e.getKey().substring(0, e.getKey().length() - 6);
            parsed.put(loggerName, e.getValue());
        }

        return parsed;
    }
}
