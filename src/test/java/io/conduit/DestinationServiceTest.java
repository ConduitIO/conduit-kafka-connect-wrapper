package io.conduit;

import io.conduit.grpc.Destination;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DestinationServiceTest {
    private DestinationService underTest;
    @Mock
    private TaskFactory taskFactory;
    @Mock
    private SinkTask task;
    @Mock
    private StreamObserver<Destination.Start.Response> startStream;
    @Mock
    private StreamObserver<Destination.Configure.Response> cfgStream;

    @BeforeEach
    public void setUp() {
        this.underTest = new DestinationService(taskFactory);
    }

    @Test
    @DisplayName("SinkTask with required schema created")
    public void testCreateTaskSchemaRequired() {
        when(taskFactory.newSinkTask("io.aiven.connect.jdbc.sink.JdbcSinkTask")).thenReturn(task);

        underTest.configure(
                newConfigRequest(Map.of(
                        "task.class", "io.aiven.connect.jdbc.sink.JdbcSinkTask",
                        "schema", "{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":true,\"field\":\"id\"}],\"optional\":false,\"name\":\"test_schema\"}"
                )),
                cfgStream
        );

        verify(cfgStream).onNext(any(Destination.Configure.Response.class));
        verify(cfgStream).onCompleted();
    }

    @Test
    @DisplayName("SinkTask created, schema not required")
    public void testCreateTaskSchemaNotRequired() {
        when(taskFactory.newSinkTask("io.foo.bar")).thenReturn(task);

        underTest.configure(
                newConfigRequest(Map.of("task.class", "io.foo.bar")),
                cfgStream
        );

        verify(cfgStream).onNext(any(Destination.Configure.Response.class));
        verify(cfgStream).onCompleted();
    }

    @Test
    @DisplayName("Start task with correct config.")
    public void testStartTask() {
        when(taskFactory.newSinkTask("io.foo.bar")).thenReturn(task);
        underTest.configure(
                newConfigRequest(Map.of("task.class", "io.foo.bar", "another.param", "another.value")),
                cfgStream
        );
        underTest.start(newStartRequest(), startStream);
        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(task).start(propsCaptor.capture());
        propsCaptor.getValue().equals(Map.of("another.param", "another.value"));

        verify(startStream).onNext(any(Destination.Start.Response.class));
        verify(startStream).onCompleted();
    }

    private Destination.Start.Request newStartRequest() {
        return Destination.Start.Request.newBuilder().build();
    }

    private Destination.Configure.Request newConfigRequest(Map<String, String> config) {
        return Destination.Configure.Request.newBuilder()
                .putAllConfig(config)
                .build();
    }
}