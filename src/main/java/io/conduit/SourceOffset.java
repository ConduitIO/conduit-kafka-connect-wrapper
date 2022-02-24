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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import static java.util.Collections.emptyMap;

/**
 * A wrapper for an offset map in a Kafka source connector.
 */
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
