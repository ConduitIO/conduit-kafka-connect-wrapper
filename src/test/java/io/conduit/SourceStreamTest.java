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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
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
    private StreamObserver<Source.Run.Response> streamObserver;
    @Mock
    private Function<SourceRecord, Record> transformer;

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void testRunAfterInit() throws InterruptedException {
        new SourceStream(task, streamObserver, transformer);
        Thread.sleep(50);
        verify(task, atLeastOnce()).poll();
    }

    @Test
    public void testOnCompleted() {
        var underTest = new SourceStream(task, streamObserver, transformer);
        underTest.onCompleted();

        verify(streamObserver).onCompleted();
    }

    @Test
    public void testWaitForRecords() throws InterruptedException {
        SourceRecord sourceRec = mock(SourceRecord.class);
        Record conduitRec = testConduitRec();

        when(task.poll()).thenReturn(
                null,
                emptyList(),
                null,
                emptyList(),
                List.of(sourceRec),
                null
        );
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);

        new SourceStream(task, streamObserver, transformer);
        Thread.sleep(50);

        ArgumentCaptor<Source.Run.Response> responseCaptor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, never()).onError(any());
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals(conduitRec, responseCaptor.getValue().getRecord());
    }

    @SneakyThrows
    @Test
    public void testCannotReadRecord() {
        testConnectorTaskThrows(new RuntimeException("surprised ya, huh?"));
    }

    @SneakyThrows
    @Test
    public void testSourceTaskThrowsAnError() {
        testConnectorTaskThrows(new Error("surprised ya, huh?"));
    }

    private void testConnectorTaskThrows(Throwable surprise) throws InterruptedException {
        when(task.poll()).thenThrow(surprise);
        new SourceStream(task, streamObserver, transformer);
        Thread.sleep(50);

        verify(streamObserver, never()).onNext(any());

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(streamObserver, atLeastOnce()).onError(captor.capture());
        Throwable t = captor.getValue();
        assertInstanceOf(StatusException.class, t);
        assertEquals(surprise, t.getCause());
    }

    private Record testConduitRec() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .build();
    }
}
