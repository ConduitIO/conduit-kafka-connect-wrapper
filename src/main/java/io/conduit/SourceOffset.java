package io.conduit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

import static java.util.Collections.emptyMap;

@EqualsAndHashCode
@Getter
@Setter
public class SourceOffset {
    private final Map<String, ?> map;

    public SourceOffset(Map<String, ?> map) {
        this.map = map;
    }

    public SourceOffset() {
        this(emptyMap());
    }

    public Map<String, Object> asMap() {
        return (Map<String, Object>) map;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return Utils.isEmpty(map);
    }
}
