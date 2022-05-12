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

import java.util.Arrays;
import java.util.NoSuchElementException;

import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

/**
 * A {@link TaskFactory} implementation using the class loader of this class
 * to find the requested tasks.
 */
public class ClasspathTaskFactory implements TaskFactory {
    @SneakyThrows
    @Override
    public SinkTask newSinkTask(String connectorClass) {
        SinkConnector connector = (SinkConnector) newInstance(connectorClass);
        return (SinkTask) newInstance(connector.taskClass().getCanonicalName());
    }

    @SneakyThrows
    @Override
    public SourceTask newSourceTask(String connectorClass) {
        SourceConnector connector = (SourceConnector) newInstance(connectorClass);
        return (SourceTask) newInstance(connector.taskClass().getCanonicalName());
    }

    @SneakyThrows
    private Object newInstance(String className) {
        Class<?> clazz = Class.forName(className);
        return Arrays.stream(clazz.getConstructors())
                .filter(c -> c.getParameterCount() == 0)
                .findFirst()
                // get() also throws NoSuchElementException, but here:
                // 1. we send the caller more details
                // 2. we also adhere to a best practice (not calling get() without checking if value is present)
                .orElseThrow(() -> new NoSuchElementException("no parameterless constructor for " + className))
                .newInstance();
    }
}
