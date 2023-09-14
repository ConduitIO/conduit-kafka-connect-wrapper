/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DefaultDestinationStreamTest {
    @Mock
    private SinkTask task;
    @Mock
    private SchemaProvider schemaProvider;
    @Mock
    private StreamObserver<Destination.Run.Response> responseObserver;

    @Test
    public void testRecordWrittenSuccesfully() {
        var underTest = new DefaultDestinationStream(
            task,
            schemaProvider,
            responseObserver
        );

        var position = ByteString.copyFromUtf8("test-position");
        var record = Record.newBuilder()
            .setKey(
                Data.newBuilder()
                    .setRawData(ByteString.copyFromUtf8("test-key"))
                    .build()
            )
            .setPosition(position)
            .setPayload(
                Change.newBuilder()
                    .setAfter(Data.newBuilder()
                        .setRawData(ByteString.copyFromUtf8("test-raw-data"))
                        .build())
                    .build()
            )
            .build();
        var request = Destination.Run.Request.newBuilder()
            .setRecord(record)
            .build();

        underTest.onNext(request);

        var captor = ArgumentCaptor.forClass(List.class);
        verify(task).put(captor.capture());
        assertEquals(1, captor.getValue().size());
        assertInstanceOf(SinkRecord.class, captor.getValue().get(0));
        var actualSinkRecord = (SinkRecord) captor.getValue().get(0);

        // Sink record verifications
        assertNull(actualSinkRecord.topic());
        // Key
        assertEquals(Schema.STRING_SCHEMA, actualSinkRecord.keySchema());
        assertEquals("test-key", actualSinkRecord.key());

        // Value
        assertNull(actualSinkRecord.valueSchema());
        assertInstanceOf(byte[].class, actualSinkRecord.value());
        assertArrayEquals(
            "test-raw-data".getBytes(StandardCharsets.UTF_8),
            (byte[]) actualSinkRecord.value()
        );

        verify(responseObserver).onNext(Destination.Run.Response
            .newBuilder()
            .setAckPosition(position)
            .build()
        );
    }

    @Test
    public void testOnError() {
        var underTest = new DefaultDestinationStream(
            task,
            schemaProvider,
            responseObserver
        );

        var t = new RuntimeException("boom!");
        underTest.onError(t);

        var captor = ArgumentCaptor.forClass(Throwable.class);
        verify(responseObserver).onError(captor.capture());
        assertEquals(t, captor.getValue().getCause());
    }

    @Test
    public void testOnCompleted() {
        var underTest = new DefaultDestinationStream(
            task,
            schemaProvider,
            responseObserver
        );

        underTest.onCompleted();
        verify(responseObserver).onCompleted();
    }
}