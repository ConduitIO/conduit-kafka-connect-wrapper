package io.conduit;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

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
