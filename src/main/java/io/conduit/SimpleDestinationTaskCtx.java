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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;


/**
 * A {@link SinkTaskContext} implementation which also provides
 * the source task's configuration and position (a mapping from partitions to offsets).
 */
public class SimpleDestinationTaskCtx implements SinkTaskContext {
    private final Map<String, String> config;
    private Map<TopicPartition, Long> resetTopicPartitioOffset;

    public SimpleDestinationTaskCtx(Map<String, String> config) {
        this.config = config;
        this.resetTopicPartitioOffset = new HashMap<TopicPartition, Long>();
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        resetTopicPartitioOffset.put(tp, offset);
    }

    @Override
    public void timeout(long timeoutMs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return null;
    }

    @Override
    public void pause(TopicPartition... partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(TopicPartition... partitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void requestCommit() {
        throw new UnsupportedOperationException();
    }

    public boolean isReset(TopicPartition tp, long offset) {
        return resetTopicPartitioOffset.containsKey(tp)
                && resetTopicPartitioOffset.get(tp) == offset;
    }

    public void ackResetOffset(TopicPartition tp, long offset) {
        resetTopicPartitioOffset.remove(tp, offset);
    }
}
