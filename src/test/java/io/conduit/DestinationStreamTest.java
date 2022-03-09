package io.conduit;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DestinationStreamTest {
    private DestinationStream underTest;
    @Mock
    private SinkTask task;
    @Mock
    private Schema schema;
    @Mock
    private StreamObserver<Destination.Run.Response> streamObserver;

    @BeforeEach
    public void setUp() {
        schema = new SchemaBuilder(Schema.Type.STRUCT)
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        this.underTest = new DestinationStream(task, new FixedSchemaProvider(schema), streamObserver);
    }

    @Test
    public void testWriteRecordNoSchema() {
        DestinationStream underTest = new DestinationStream(task, new FixedSchemaProvider(null), streamObserver);
        Destination.Run.Request request = newRequest();
        Record record = request.getRecord();

        underTest.onNext(request);

        ArgumentCaptor<Collection<SinkRecord>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(recordsCaptor.capture());

        // verify that SinkRecord has correct content: schema + key + payload
        // todo check timestamps too?
        SinkRecord sinkRecord = recordsCaptor.getValue().iterator().next();
        assertNull(sinkRecord.valueSchema());
        assertEquals(record.getKey().getRawData().toStringUtf8(), sinkRecord.key());
        assertEquals(
                record.getPayload().getRawData(),
                ByteString.copyFrom((byte[]) sinkRecord.value())
        );

        // task should be flushed, since Conduit doesn't (yet) support async. writes
        verify(task).flush(argThat(Map::isEmpty));
        // no errors
        verify(streamObserver, never()).onError(any());
        // verify position
        var responseCaptor = ArgumentCaptor.forClass(Destination.Run.Response.class);
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals(record.getPosition(), responseCaptor.getValue().getAckPosition());
    }

    @Test
    public void testWriteRecordHappyPath() {
        Destination.Run.Request request = newRequest();
        Record record = request.getRecord();

        underTest.onNext(request);

        ArgumentCaptor<Collection<SinkRecord>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(recordsCaptor.capture());

        // verify that SinkRecord has correct content: schema + key + payload
        // todo check timestamps too?
        SinkRecord sinkRecord = recordsCaptor.getValue().iterator().next();
        assertEquals(schema, sinkRecord.valueSchema());
        assertEquals(record.getKey().getRawData().toStringUtf8(), sinkRecord.key());

        // task should be flushed, since Conduit doesn't (yet) support async. writes
        verify(task).flush(argThat(Map::isEmpty));

        // verify position
        verify(streamObserver, never()).onError(any());
        var responseCaptor = ArgumentCaptor.forClass(Destination.Run.Response.class);
        verify(streamObserver).onNext(responseCaptor.capture());
        assertEquals(record.getPosition(), responseCaptor.getValue().getAckPosition());
    }

    @Test
    public void testWriteRecordFlushError() {
        testWriteRecordError(
                new RuntimeException("surprised ya, huh?"),
                exception -> doThrow(exception).when(task).flush(any())
        );
    }

    @Test
    public void testWriteRecordPutError() {
        testWriteRecordError(
                new RuntimeException("surprised ya, huh?"),
                exception -> doThrow(exception).when(task).put(any())
        );
    }

    private void testWriteRecordError(Exception surprise, Consumer<Exception> errorSetup) {
        Destination.Run.Request request = newRequest();
        errorSetup.accept(surprise);

        underTest.onNext(request);

        ArgumentCaptor<Collection<SinkRecord>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(task).put(recordsCaptor.capture());
        // verify exception
        var exception = ArgumentCaptor.forClass(Exception.class);
        verify(streamObserver).onError(exception.capture());

        assertInstanceOf(StatusException.class, exception.getValue());
        StatusException statusException = (StatusException) exception.getValue();
        assertEquals(Status.Code.INTERNAL, statusException.getStatus().getCode());
        assertEquals(surprise, statusException.getCause());
    }

    private Destination.Run.Request newRequest() {
        return Destination.Run.Request.newBuilder()
                .setRecord(newRecord())
                .build();
    }

    private Record newRecord() {
        return Record.newBuilder()
                .setKey(Data.newBuilder().setRawData(ByteString.copyFromUtf8(UUID.randomUUID().toString())).build())
                .setPayload(Data.newBuilder().setRawData(newRecordPayload()).build())
                .setPosition(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(123456).build())
                .build();
    }

    private ByteString newRecordPayload() {
        return ByteString.copyFromUtf8(
                "{\"id\":123,\"name\":\"foobar\"}"
        );
    }
}