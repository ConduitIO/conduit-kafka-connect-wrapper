package io.conduit;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceTask;

public interface TaskFactory {
    SinkTask newSinkTask(String className);

    SourceTask newSourceTask(String className);
}
