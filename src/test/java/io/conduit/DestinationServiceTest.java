package io.conduit;

import java.util.Map;

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
    @DisplayName("Cannot provide schema and auto-generate schema at same time")
    public void testProvideSchemaRequestAutogenerate() {
        when(taskFactory.newSinkTask("io.foo.bar")).thenReturn(task);

        underTest.configure(
                newConfigRequest(Map.of(
                        "wrapper.task.class", "io.foo.bar",
                        "wrapper.schema", "{\"type\":\"struct\",\"fields\":[{\"type\":\"boolean\",\"optional\":true,\"field\":\"joined\"}],\"name\":\"customers\"}",
                        "wrapper.schema.autogenerate.enabled", "true"
                )),
                cfgStream
        );

        var captor = ArgumentCaptor.forClass(Throwable.class);
        verify(cfgStream).onError(captor.capture());
        assertEquals(
                "INTERNAL: couldn't configure task: You cannot provide a schema and use schema auto-generation at the same time.",
                captor.getValue().getMessage()
        );
    }

    @Test
    @DisplayName("Cannot provide schema and auto-generate schema at same time")
    public void testSchemaAutogenerationNameRequired() {
        when(taskFactory.newSinkTask("io.foo.bar")).thenReturn(task);

        underTest.configure(
                newConfigRequest(Map.of(
                        "wrapper.task.class", "io.foo.bar",
                        "wrapper.schema.autogenerate.enabled", "true"
                )),
                cfgStream
        );

        var captor = ArgumentCaptor.forClass(Throwable.class);
        verify(cfgStream).onError(captor.capture());
        assertEquals(
                "INTERNAL: couldn't configure task: Schema name not provided",
                captor.getValue().getMessage()
        );
    }

    @Test
    @DisplayName("Start task with correct config.")
    public void testStartTask() {
        when(taskFactory.newSinkTask("io.foo.bar")).thenReturn(task);
        underTest.configure(
                newConfigRequest(Map.of("wrapper.task.class", "io.foo.bar", "another.param", "another.value")),
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
