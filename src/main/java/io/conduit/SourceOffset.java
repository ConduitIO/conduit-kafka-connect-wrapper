package io.conduit;

import lombok.EqualsAndHashCode;

import java.util.Map;

import static java.util.Collections.emptyMap;

@EqualsAndHashCode
public class SourceOffset {
    private final Map<String, Object> map;

    public SourceOffset(Map<String, Object> map) {
        this.map = map;
    }

    public SourceOffset() {
        this(emptyMap());
    }

    public Map<String, Object> asMap() {
        return map;
    }

    public boolean isEmpty() {
        return Utils.isEmpty(map);
    }
}
