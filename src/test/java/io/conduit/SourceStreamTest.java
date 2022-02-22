package io.conduit;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Data;
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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SourceStreamTest {
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
    public void testRunAfterInit() throws InterruptedException {
        new SourceStream(task, position, streamObserver, transformer);
        Thread.sleep(50);
        verify(task, atLeastOnce()).poll();
    }

    @Test
    @DisplayName("When onCompleted is called, the underlying streams's onCompleted is called.")
    public void testOnCompleted() {
        var underTest = new SourceStream(task, position, streamObserver, transformer);
        underTest.onCompleted();

        verify(streamObserver).onCompleted();
    }

    @Test
    @DisplayName("Wait until records are available.")
    public void testWaitForRecords() throws InterruptedException {
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

        new SourceStream(task, position, streamObserver, transformer);
        Thread.sleep(250);

        var responseCaptor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, never()).onError(any());
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals(conduitRec.build(), responseCaptor.getValue().getRecord());
    }

    @SneakyThrows
    @Test
    @DisplayName("When reading a record throws an exception, then exception is handled.")
    public void testCannotReadRecord() {
        testConnectorTaskThrows(new RuntimeException("surprised ya, huh?"));
    }

    @SneakyThrows
    @Test
    @DisplayName("When reading a record throws an error, then exception is error.")
    public void testSourceTaskThrowsAnError() {
        testConnectorTaskThrows(new Error("surprised ya, huh?"));
    }

    private void testConnectorTaskThrows(Throwable surprise) throws InterruptedException {
        when(task.poll()).thenThrow(surprise);

        new SourceStream(task, position, streamObserver, transformer);
        Thread.sleep(100);

        verify(streamObserver, never()).onNext(any());

        var captor = ArgumentCaptor.forClass(Throwable.class);
        verify(streamObserver, atLeastOnce()).onError(captor.capture());
        Throwable t = captor.getValue();
        assertInstanceOf(StatusException.class, t);
        assertEquals(surprise, t.getCause());
    }

    @SneakyThrows
    @Test
    @DisplayName("Positions from records from different partitions (tables) are correctly merged.")
    public void testPositionsMerged() {
        var sr1 = mockSourceRec(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        var sr2 = mockSourceRec(Map.of("p2", "p2-value"), Map.of("o2", "o2-value"));
        var cr1 = testConduitRec();
        var cr2 = testConduitRec();

        when(task.poll()).thenReturn(List.of(sr1, sr2), null);
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));
        when(transformer.apply(sr1)).thenReturn(cr1);
        when(transformer.apply(sr2)).thenReturn(cr2);

        new SourceStream(task, position, streamObserver, transformer);
        Thread.sleep(500);
        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, times(2)).onNext(captor.capture());

        verify(position).add(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        verify(position).add(Map.of("p2", "p2-value"), Map.of("o2", "o2-value"));
    }

    private SourceRecord mockSourceRec(Map<String, ?> partition, Map<String, ?> offset) {
        SourceRecord mock = mock(SourceRecord.class);
        doReturn(partition).when(mock).sourcePartition();
        doReturn(offset).when(mock).sourceOffset();

        return mock;
    }

    private Record.Builder testConduitRec() {
        return Record.newBuilder()
                .setPayload(
                        Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build()
                );
    }
}
