package io.conduit;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class SimpleSourceTaskCtx implements SourceTaskContext {
    private final Map<String, String> config;
    private final Map<String, Object> sourcePartition;
    private final Map<String, Object> sourceOffset;

    public SimpleSourceTaskCtx(Map<String, String> config, Map<String, Object> sourcePartition, Map<String, Object> sourceOffset) {
        this.config = config;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
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
                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                for (Map<String, T> partition : partitions) {
                    if (partition.equals(sourcePartition)) {
                        return Map.of(partition, sourceOffset);
                    }
                }
                return emptyMap();
            }
        };
    }
}
