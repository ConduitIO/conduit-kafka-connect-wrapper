package io.conduit;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultSourceStreamTest {
    @Mock
    private SourceTask task;
    @Mock
    private SourcePosition position;
    @Mock
    private StreamObserver<Source.Run.Response> streamObserver;
    @Mock
    private Function<SourceRecord, Record.Builder> transformer;

    @Test
    @DisplayName("When SourceStream is created, the underlying SourceTask starts being polled.")
    void testRunAfterInit() throws InterruptedException {
        new DefaultSourceStream(task, position, streamObserver, transformer).startAsync();
        verify(task, timeout(50).atLeastOnce()).poll();
    }

    @Test
    @DisplayName("When onCompleted is called, the underlying streams's onCompleted is called.")
    void testOnCompleted() {
        var underTest = new DefaultSourceStream(task, position, streamObserver, transformer);
        underTest.onCompleted();

        verify(streamObserver).onCompleted();
    }

    @Test
    @DisplayName("Wait until records are available.")
    void testWaitForRecords() throws InterruptedException {
        var sourceRec = mockSourceRec(Map.of(), Map.of());
        var conduitRec = testConduitRec();

        when(task.poll()).thenReturn(
            null,
            emptyList(),
            null,
            emptyList(),
            List.of(sourceRec),
            null
        );
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);

        new DefaultSourceStream(task, position, streamObserver, transformer).startAsync();

        var responseCaptor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, timeout(1000)).onNext(responseCaptor.capture());
        verify(streamObserver, never()).onError(any());

        assertEquals(conduitRec.build(), responseCaptor.getValue().getRecord());
    }

    @SneakyThrows
    @Test
    @DisplayName("When reading a record throws an exception, then exception is handled.")
    void testCannotReadRecord() {
        testConnectorTaskThrows(new RuntimeException("surprised ya, huh?"));
    }

    @SneakyThrows
    @Test
    @DisplayName("When reading a record throws an error, then exception is error.")
    void testSourceTaskThrowsAnError() {
        testConnectorTaskThrows(new Error("surprised ya, huh?"));
    }

    private void testConnectorTaskThrows(Throwable surprise) throws InterruptedException {
        when(task.poll()).thenThrow(surprise);

        new DefaultSourceStream(task, position, streamObserver, transformer).startAsync();

        var captor = ArgumentCaptor.forClass(Throwable.class);
        verify(streamObserver, timeout(100).atLeastOnce()).onError(captor.capture());
        verify(streamObserver, never()).onNext(any());

        Throwable t = captor.getValue();
        assertInstanceOf(StatusException.class, t);
        assertEquals(surprise, t.getCause());
    }

    @SneakyThrows
    @Test
    @DisplayName("Positions from records from different partitions (tables) are correctly merged.")
    void testPositionsMerged() {
        var sr1 = mockSourceRec(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        var sr2 = mockSourceRec(Map.of("p2", "p2-value"), Map.of("o2", "o2-value"));
        var cr1 = testConduitRec();
        var cr2 = testConduitRec();

        when(task.poll()).thenReturn(List.of(sr1, sr2), null);
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));
        when(transformer.apply(sr1)).thenReturn(cr1);
        when(transformer.apply(sr2)).thenReturn(cr2);

        new DefaultSourceStream(task, position, streamObserver, transformer).startAsync();
        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, timeout(500).times(2)).onNext(captor.capture());

        verify(position).add(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        verify(position).add(Map.of("p2", "p2-value"), Map.of("o2", "o2-value"));
    }

    @Test
    @DisplayName("Fail when ack is called with an unknown position")
    void testCommitOnAck_UnknownPosition() throws InterruptedException {
        var sourceRec = mockSourceRec(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        var conduitRec = testConduitRec();

        when(task.poll()).thenReturn(List.of(sourceRec), List.of());
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));

        DefaultSourceStream underTest = new DefaultSourceStream(
            task,
            position,
            streamObserver,
            transformer
        );

        underTest.startAsync();

        // DefaultSourceStream has a worker thread internally
        // so we need to give it a bit of time to start polling
        verify(streamObserver, timeout(200)).onNext(any());

        var ackRequest = Source.Run.Request.newBuilder()
            .setAckPosition(ByteString.copyFromUtf8("unknown"))
            .build();
        underTest.onNext(ackRequest);

        verify(streamObserver).onError(any());
        verify(task, never()).commitRecord(any(), any());
        verify(task, never()).commit();
    }

    @Test
    @DisplayName("When acking, commit records to the underlying source task")
    void testCommitOnAck() throws InterruptedException {
        var sourceRec = mockSourceRec(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        var conduitRec = testConduitRec();

        when(task.poll()).thenReturn(List.of(sourceRec), List.of());
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));

        DefaultSourceStream underTest = new DefaultSourceStream(
            task,
            position,
            streamObserver,
            transformer
        );

        underTest.startAsync();

        // DefaultSourceStream has a worker thread internally
        // so we need to give it a bit of time to start polling
        verify(streamObserver, timeout(200_000)).onNext(any());
        verify(streamObserver, never()).onError(any());

        var ackRequest = Source.Run.Request.newBuilder()
            .setAckPosition(position.asByteString())
            .build();
        underTest.onNext(ackRequest);

        verify(task).commitRecord(sourceRec, null);
        verify(task).commit();
    }

    private SourceRecord mockSourceRec(Map<String, ?> partition, Map<String, ?> offset) {
        SourceRecord mock = mock(SourceRecord.class);
        doReturn(partition).when(mock).sourcePartition();
        doReturn(offset).when(mock).sourceOffset();
        doReturn("test-value").when(mock).value();

        return mock;
    }

    private Record.Builder testConduitRec() {
        return Record.newBuilder()
            .setPayload(TestUtils.newRecordPayload(UUID.randomUUID().toString()));
    }
}
