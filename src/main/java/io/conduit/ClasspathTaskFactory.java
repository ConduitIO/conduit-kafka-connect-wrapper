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

import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceTask;

/**
 * A {@link TaskFactory} implementation using the class loader of this class
 * to find the requested tasks.
 */
public class ClasspathTaskFactory implements TaskFactory {
    @SneakyThrows
    @Override
    public SinkTask newSinkTask(String className) {
        return (SinkTask) newInstance(className);
    }

    @SneakyThrows
    @Override
    public SourceTask newSourceTask(String className) {
        return (SourceTask) newInstance(className);
    }

    @SneakyThrows
    private Object newInstance(String className) {
        Class<?> clazz = Class.forName(className);
        Object taskObj = Arrays.stream(clazz.getConstructors())
                .filter(c -> c.getParameterCount() == 0)
                .findFirst()
                .get()
                .newInstance();
        return taskObj;
    }
}
