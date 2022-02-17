package io.conduit;

import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode
public class SourcePartition {
    private final Map<String, Object> map;

    public SourcePartition(Map<String, Object> map) {
        this.map = map;
    }
}
