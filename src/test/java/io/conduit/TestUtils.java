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

import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Source;
import io.grpc.stub.StreamObserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

 class TestUtils {
    static Source.Configure.Request newConfigRequest(Map<String, String> config) {
        return Source.Configure.Request.newBuilder()
                .putAllConfig(config)
                .build();
    }

    static Change newCreatedRecord(Struct struct) {
        return Change.newBuilder()
                .setAfter(Data.newBuilder().setStructuredData(struct).build())
                .build();
    }

    static Change newCreatedRecord(byte[] bytes) {
        Data data = Data.newBuilder()
                .setRawData(ByteString.copyFrom(bytes))
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }

    static Change newCreatedRecord(ObjectNode json) {
        Data data = Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8(json.toString()))
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }

    static Change newRecordPayload() {
        return newRecordPayload("{\"id\":123,\"name\":\"foobar\"}");
    }

    static Change newRecordPayload(String s) {
        var data = Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8(s))
                .build();
        return Change.newBuilder()
                .setAfter(data)
                .build();
    }

    static StreamObserver run(SourceService service, Map<String, String> config) {
        StreamObserver cfgStream = mock(StreamObserver.class);
        service.configure(
            TestUtils.newConfigRequest(config),
            cfgStream
        );
        verify(cfgStream, never()).onError(any());

        StreamObserver startStream = mock(StreamObserver.class);
        service.start(
            Source.Start.Request.newBuilder().build(),
            startStream
        );
        verify(startStream, never()).onError(any());

        StreamObserver runStream = mock(StreamObserver.class);
        service.run(runStream);

        return runStream;
    }
}
