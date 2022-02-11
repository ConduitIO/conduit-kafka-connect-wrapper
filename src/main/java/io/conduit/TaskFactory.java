package io.conduit;

import org.apache.kafka.connect.sink.SinkTask;

public interface TaskFactory {
    SinkTask newSinkTask(String clazz);
}
