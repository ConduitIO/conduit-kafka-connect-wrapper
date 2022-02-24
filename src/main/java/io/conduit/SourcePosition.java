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
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.protobuf.ByteString;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * Represents a position in a Kafka source connector.
 */
@EqualsAndHashCode
@Getter
@Setter
public class SourcePosition {
    // Keys are partitions as JSON strings (so that they can be easily deserialized),
    // and values are offsets, as defined by the Kafka Connect API.
    // For more information, see the documentation for org.apache.kafka.connect.source.SourceRecord.
    @JsonDeserialize(keyUsing = SourcePartitionKeyDeser.class)
    private final Map<SourcePartition, SourceOffset> positions = new HashMap<>();

    /**
     * Parses a string position into a POJO.
     */
    @SneakyThrows
    public static SourcePosition fromString(String position) {
        if (Utils.isEmpty(position)) {
            return new SourcePosition();
        }
        return Utils.mapper.readValue(position, SourcePosition.class);
    }

    @SneakyThrows
    public void add(Map<String, ?> partition, Map<String, ?> offset) {
        positions.put(new SourcePartition(partition), new SourceOffset(offset));
    }

    /**
     * Returns this position as a JSON string, wrapped in a {@link ByteString}.
     */
    @SneakyThrows
    public ByteString asByteString() {
        return ByteString.copyFrom(
                Utils.mapper.writeValueAsBytes(this)
        );
    }

    public SourceOffset offsetFor(Map<String, ?> partition) {
        SourceOffset offset = positions.get(new SourcePartition(partition));
        return offset != null ? offset : new SourceOffset();
    }

    private static class SourcePartitionKeyDeser extends KeyDeserializer {

        @Override
        public Object deserializeKey(String s, DeserializationContext deserializationContext) throws IOException {
            return Utils.mapper.readValue(s, SourcePartition.class);
        }
    }
}
