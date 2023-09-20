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
import lombok.Getter;
import lombok.Setter;

import static io.conduit.Utils.mapper;

/**
 * Represents a basis for source or destination configurations.
 * Contains the configuration for the Conduit connector and the underlying Kafka connector.
 * Conduit connector configuration parameters are prefixed with <code>wrapper.</code>.
 */
@Getter
@Setter
public class Config {
    public static final String WRAPPER_PREFIX = "wrapper.";

    @JsonProperty("connector.class")
    private String connectorClass;
    @JsonProperty("log.level")
    private String logLevel;
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
        Map<String, String> connectorMap = new HashMap<>();

        map.forEach((k, v) -> {
            if (k.startsWith(WRAPPER_PREFIX)) {
                wrapperMap.put(k.replaceFirst(WRAPPER_PREFIX, ""), v);
            } else {
                connectorMap.put(k, v);
            }
        });
        T cfg = mapper.convertValue(wrapperMap, clazz);
        cfg.setKafkaConnectorCfg(connectorMap);
        return cfg;
    }
}
