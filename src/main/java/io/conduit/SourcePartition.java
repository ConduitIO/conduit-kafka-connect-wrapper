package io.conduit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.util.Map;

import static java.util.Collections.emptyMap;

@EqualsAndHashCode
@Getter
@Setter
public class SourcePartition {
    private final Map<String, ?> map;

    public SourcePartition() {
        this(emptyMap());
    }

    public SourcePartition(Map<String, ?> map) {
        this.map = map;
    }

    @SneakyThrows
    @Override
    public String toString() {
        return Utils.mapper.writeValueAsString(this);
    }
}
