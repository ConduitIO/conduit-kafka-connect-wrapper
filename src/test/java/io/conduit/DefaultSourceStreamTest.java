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
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.stub.StreamObserver;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;

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
    void testRecordReadSuccesfully() throws InterruptedException {
        // SourcePosition position = new SourcePosition();

        var sourceRec = mockSourceRec(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        var conduitRec = testConduitRec();

        when(task.poll()).thenReturn(List.of(sourceRec), List.of());
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);
        when(transformer.apply(sourceRec)).thenReturn(conduitRec);

        doNothing().when(position).add(Map.of("p1", "p1-value"), Map.of("o1", "o1-value"));
        when(position.asByteString()).thenReturn(ByteString.copyFromUtf8("irrelevant"));

        DefaultSourceStream ds = new DefaultSourceStream(
                task,
                position,
                streamObserver,
                transformer
        );

        doAnswer(invocation -> {
            ds.onNext(null);
            return null;
        }).when(streamObserver).onNext(any());

        ds.doPoll();

        verify(streamObserver, atLeastOnce()).onNext(any());
        verify(streamObserver, never()).onError(any());
        verify(position).add(any(), any());
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
