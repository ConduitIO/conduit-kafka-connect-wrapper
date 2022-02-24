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
