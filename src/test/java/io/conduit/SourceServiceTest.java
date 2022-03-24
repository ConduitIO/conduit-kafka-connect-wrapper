package io.conduit;

import java.util.Map;

import io.conduit.grpc.Source;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SourceServiceTest {
    private SourceService underTest;
    @Mock
    private TaskFactory taskFactory;
    @Mock
    private SourceTask task;

    @BeforeEach
    public void setUp() {
        underTest = new SourceService(taskFactory);
    }

    @Test
    @DisplayName("When the configuration is OK, then a successful response is sent back.")
    public void testConfigureOk() {
        Source.Configure.Request request = Source.Configure.Request.newBuilder().build();
        StreamObserver<Source.Configure.Response> streamObserver = mock(StreamObserver.class);

        underTest.configure(request, streamObserver);

        verify(streamObserver).onNext(any());
        verify(streamObserver, never()).onError(any());
    }

    @Test
    @DisplayName("When the configuration is NOT OK, then an error response is sent back.")
    public void testConfigureWithError() {
        Source.Configure.Request request = Source.Configure.Request.newBuilder().build();
        StreamObserver<Source.Configure.Response> streamObserver = mock(StreamObserver.class);
        RuntimeException exception = new RuntimeException("surprised ya, huh?");
        when(taskFactory.newSourceTask(any())).thenThrow(exception);
        underTest.configure(request, streamObserver);

        verify(streamObserver, never()).onNext(any());
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(streamObserver).onError(captor.capture());
        assertInstanceOf(StatusException.class, captor.getValue());
        assertEquals(exception, captor.getValue().getCause());
    }

    @Test
    @DisplayName("Start task with correct config.")
    public void testStartTask() {
        when(taskFactory.newSourceTask("io.foo.bar")).thenReturn(task);
        underTest.configure(
                newConfigRequest(Map.of("wrapper.task.class", "io.foo.bar", "another.param", "another.value")),
                mock(StreamObserver.class)
        );

        StreamObserver<Source.Start.Response> startStream = mock(StreamObserver.class);
        underTest.start(newStartRequest(), startStream);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(task).start(propsCaptor.capture());
        propsCaptor.getValue().equals(Map.of("another.param", "another.value"));

        verify(startStream).onNext(any(Source.Start.Response.class));
        verify(startStream).onCompleted();
    }

    @Test
    @DisplayName("Start task throws an exception.")
    public void testCannotStartTask() {
        when(taskFactory.newSourceTask("io.foo.bar")).thenReturn(task);
        RuntimeException exception = new RuntimeException("surprised ya, huh?");
        doThrow(exception).when(task).start(anyMap());

        underTest.configure(
                newConfigRequest(Map.of("wrapper.task.class", "io.foo.bar", "another.param", "another.value")),
                mock(StreamObserver.class)
        );

        StreamObserver<Source.Start.Response> startStream = mock(StreamObserver.class);
        underTest.start(newStartRequest(), startStream);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(task).start(propsCaptor.capture());
        propsCaptor.getValue().equals(Map.of("another.param", "another.value"));

        verify(startStream, never()).onNext(any());
        ArgumentCaptor<Throwable> tCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(startStream).onError(tCaptor.capture());
        assertInstanceOf(StatusException.class, tCaptor.getValue());
        assertEquals(exception, tCaptor.getValue().getCause());
    }

    private Source.Start.Request newStartRequest() {
        return Source.Start.Request.newBuilder().build();
    }

    private Source.Configure.Request newConfigRequest(Map<String, String> config) {
        return Source.Configure.Request.newBuilder()
                .putAllConfig(config)
                .build();
    }
}