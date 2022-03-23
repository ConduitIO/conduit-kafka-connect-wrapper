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

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.connect.data.Schema;

import static io.conduit.Utils.mapper;

/**
 * Represents a destination configuration.
 * Contains the configuration for a Conduit destination connector and the underlying Kafka connector's configuration.
 * Conduit connector configuration parameters are prefixed with <code>wrapper.</code>.
 */
@Getter
@Setter
public class DestinationConfig extends Config {
    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonProperty("schema")
    private Schema schema;

    @JsonProperty("schema.autogenerate.enabled")
    private boolean schemaAutogenerate;

    @JsonDeserialize(using = SchemaDeserializer.class)
    @JsonProperty("schema.autogenerate.overrides")
    private Schema overrides;

    @JsonProperty("schema.autogenerate.name")
    private String schemaName;

    /**
     * Creates a new <code>DestinationConfig</code> instance from a map with configuration parameters.
     */
    public static DestinationConfig fromMap(Map<String, String> map) {
        if (Utils.isEmpty(map)) {
            return new DestinationConfig();
        }
        return fromMap(map, DestinationConfig.class);
    }

    public boolean hasSchema() {
        return getSchema() != null;
    }

    private static class SchemaDeserializer extends JsonDeserializer<Schema> {
        @Override
        public Schema deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode schemaJson = mapper.readTree(p.getValueAsString());
            return Utils.jsonConv.asConnectSchema(schemaJson);
        }
    }
}
