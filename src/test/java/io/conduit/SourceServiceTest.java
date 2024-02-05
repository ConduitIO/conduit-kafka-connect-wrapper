package io.conduit;

import java.util.LinkedList;
import java.util.Map;

import io.conduit.grpc.Source;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SourceServiceTest {
    private SourceService underTest;
    @Mock
    private TaskFactory taskFactory;
    @Mock
    private SourceTask task;

    @BeforeEach
    void setUp() {
        underTest = new SourceService(taskFactory);
    }

    @Test
    @DisplayName("When the configuration is OK, then a successful response is sent back.")
    void testConfigureOk() {
        Source.Configure.Request request = Source.Configure.Request.newBuilder().build();
        StreamObserver<Source.Configure.Response> streamObserver = mock(StreamObserver.class);

        underTest.configure(request, streamObserver);

        verify(streamObserver).onNext(any());
        verify(streamObserver, never()).onError(any());
    }

    @Test
    @DisplayName("When the configuration is NOT OK, then an error response is sent back.")
    void testConfigureWithError() {
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
        assertEquals(
            "INTERNAL: couldn't configure task: java.lang.RuntimeException: surprised ya, huh?",
            captor.getValue().getMessage()
        );
    }

    @Test
    @DisplayName("Start task with correct config.")
    void testStartTask() {
        when(taskFactory.newSourceTask("io.foo.bar")).thenReturn(task);
        underTest.configure(
            TestUtils.newConfigRequest(Map.of(
                "wrapper.connector.class", "io.foo.bar",
                "another.param", "another.value"
            )),
            mock(StreamObserver.class)
        );

        StreamObserver<Source.Start.Response> startStream = mock(StreamObserver.class);
        underTest.start(newStartRequest(), startStream);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(task).start(propsCaptor.capture());
        assertEquals(
            propsCaptor.getValue(),
            Map.of("another.param", "another.value")
        );

        verify(startStream).onNext(any(Source.Start.Response.class));
        verify(startStream).onCompleted();
    }

    @Test
    @DisplayName("Start task throws an exception.")
    void testCannotStartTask() {
        when(taskFactory.newSourceTask("io.foo.bar")).thenReturn(task);
        RuntimeException exception = new RuntimeException("surprised ya, huh?");
        doThrow(exception).when(task).start(anyMap());

        underTest.configure(
            TestUtils.newConfigRequest(Map.of(
                "wrapper.connector.class", "io.foo.bar",
                "another.param", "another.value"
            )),
            mock(StreamObserver.class)
        );

        StreamObserver<Source.Start.Response> startStream = mock(StreamObserver.class);
        underTest.start(newStartRequest(), startStream);

        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(task).start(propsCaptor.capture());
        assertEquals(
            propsCaptor.getValue(),
            Map.of("another.param", "another.value")
        );

        verify(startStream, never()).onNext(any());
        ArgumentCaptor<Throwable> tCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(startStream).onError(tCaptor.capture());
        assertInstanceOf(StatusException.class, tCaptor.getValue());
        assertEquals(exception, tCaptor.getValue().getCause());
    }

    @SneakyThrows
    @Test
    void testStopSendsLastPosition() {
        when(taskFactory.newSourceTask("io.foo.bar")).thenReturn(task);

        var records = testKafkaRecords();
        when(task.poll()).thenReturn(records, emptyList());

        var runStream = TestUtils.run(
            underTest,
            Map.of(
                "wrapper.connector.class", "io.foo.bar",
                "another.param", "another.value"
            )
        );
        verify(runStream, timeout(1500).times(records.size())).onNext(any());
        verify(runStream, never()).onError(any());

        var stopStream = mock(StreamObserver.class);
        underTest.stop(Source.Stop.Request.newBuilder().build(), stopStream);
        verify(stopStream, never()).onError(any());
        var stopCaptor = ArgumentCaptor.forClass(Source.Stop.Response.class);
        verify(stopStream).onNext(stopCaptor.capture());

        SourcePosition expected = new SourcePosition();
        expected.add(records.getLast().sourcePartition(), records.getLast().sourceOffset());
        assertEquals(
            expected.asByteString(),
            stopCaptor.getValue().getLastPosition()
        );
    }

    private LinkedList<SourceRecord> testKafkaRecords() {
        var records = new LinkedList<SourceRecord>();
        for (int i = 0; i < 5; i++) {
            records.add(
                new SourceRecord(
                    Map.of("p", 0),
                    Map.of("offset", i),
                    "test-topic",
                    i,
                    Schema.STRING_SCHEMA,
                    "test-payload-" + i
                )
            );
        }
        return records;
    }

    @Test
    void testTeardownNoStart() {
        underTest.configure(
            TestUtils.newConfigRequest(Map.of(
                "wrapper.connector.class", "io.foo.bar",
                "another.param", "another.value"
            )),
            mock(StreamObserver.class)
        );
        StreamObserver observer = mock(StreamObserver.class);
        underTest.teardown(
            Source.Teardown.Request.newBuilder().build(),
            observer
        );
        verify(observer, never()).onError(any());
    }

    @Test
    void testTeardownWhenStartFailed() {
        var task = mock(SourceTask.class);
        doThrow(new RuntimeException("gotcha!")).when(task).start(anyMap());
        when(taskFactory.newSourceTask("io.foo.bar"))
            .thenReturn(task);

        underTest.configure(
            TestUtils.newConfigRequest(Map.of(
                "wrapper.connector.class", "io.foo.bar",
                "another.param", "another.value"
            )),
            mock(StreamObserver.class)
        );

        underTest.start(Source.Start.Request.newBuilder().build(), mock(StreamObserver.class));

        StreamObserver observer = mock(StreamObserver.class);
        underTest.teardown(
            Source.Teardown.Request.newBuilder().build(),
            observer
        );
        verify(observer, never()).onError(any());
    }

    private Source.Start.Request newStartRequest() {
        return Source.Start.Request.newBuilder().build();
    }
}