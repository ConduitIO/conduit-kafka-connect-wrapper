package io.conduit;

import java.util.Map;

import io.conduit.grpc.Destination;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    @DisplayName("Cannot set schema and schema.autogenerate to true at same time.")
    public void testSetSchemaAndSchemaAutogenerate() {
        when(taskFactory.newSinkTask("io.aiven.connect.jdbc.sink.JdbcSinkTask")).thenReturn(task);

        var e = Assertions.assertThrows(
                IllegalStateException.class,
                () -> underTest.configure(
                        newConfigRequest(Map.of(
                                "task.class", "io.aiven.connect.jdbc.sink.JdbcSinkTask",
                                "schema", "{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":true,\"field\":\"id\"}],\"optional\":false,\"name\":\"test_schema\"}",
                                "schema.autogenerate", "true"
                        )),
                        cfgStream
                )
        );
        assertEquals("You should either set the schema or have it auto-generated, but not both.", e.getMessage());
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
