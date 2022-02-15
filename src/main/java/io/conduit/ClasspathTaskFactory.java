package io.conduit;

import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceTask;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public class ClasspathTaskFactory implements TaskFactory {
    @SneakyThrows
    @Override
    public SinkTask newSinkTask(String className) {
        return (SinkTask) newInstance(className);
    }

    private Object newInstance(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Class<?> clazz = Class.forName(className);
        Object taskObj = Arrays.stream(clazz.getConstructors())
                .filter(c -> c.getParameterCount() == 0)
                .findFirst()
                .get()
                .newInstance();
        return taskObj;
    }

    @SneakyThrows
    @Override
    public SourceTask newSourceTask(String className) {
        return (SourceTask) newInstance(className);
    }
}
