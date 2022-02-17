package io.conduit;

import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Arrays;

public class ClasspathTaskFactory implements TaskFactory {
    @SneakyThrows
    @Override
    public SinkTask newSinkTask(String className) {
        Class<?> clazz = Class.forName(className);
        Object taskObj = Arrays.stream(clazz.getConstructors())
                .filter(c -> c.getParameterCount() == 0)
                .findFirst()
                .get()
                .newInstance();
        return (SinkTask) taskObj;
    }
}
