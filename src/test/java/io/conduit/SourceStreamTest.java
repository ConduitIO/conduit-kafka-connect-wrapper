package io.conduit;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Data;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.stub.StreamObserver;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SourceStreamTest {
    private SourceStream underTest;
    @Mock
    private SourceTask task;
    @Mock
    private StreamObserver<Source.Run.Response> streamObserver;
    @Mock
    private Function<SourceRecord, Record> transformer;

    @BeforeEach
    public void setUp() {
        underTest = new SourceStream(task, streamObserver, transformer);
    }

    @Test
    public void testRunAfterInit() throws InterruptedException {
        Thread.sleep(100);
        verify(task).poll();
    }

    @Test
    public void testOnCompleted() throws InterruptedException {
        underTest.onCompleted();
        verify(task, atMostOnce()).poll();
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
        Thread.sleep(1500);

        ArgumentCaptor<Source.Run.Response> responseCaptor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(streamObserver, never()).onError(any());
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals(conduitRec, responseCaptor.getValue().getRecord());
    }

    private Record testConduitRec() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .build();
    }
}
