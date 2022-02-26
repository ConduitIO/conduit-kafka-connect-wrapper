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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import static java.util.Collections.emptyMap;

/**
 * A {@link SourceTaskContext} implementation which also provides
 * the source task's configuration and position (a mapping from partitions to offsets).
 */
public class SimpleSourceTaskCtx implements SourceTaskContext {
    private final Map<String, String> config;
    private final SourcePosition position;

    public SimpleSourceTaskCtx(Map<String, String> config, SourcePosition position) {
        this.config = config;
        this.position = position;
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return position.offsetFor(partition).asMap();
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                if (Utils.isEmpty(partitions)) {
                    return emptyMap();
                }

                Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();
                for (Map<String, T> partition : partitions) {
                    SourceOffset offset = position.offsetFor(partition);
                    if (!offset.isEmpty()) {
                        offsets.put(partition, offset.asMap());
                    }
                }
                return offsets;
            }
        };
    }
}
