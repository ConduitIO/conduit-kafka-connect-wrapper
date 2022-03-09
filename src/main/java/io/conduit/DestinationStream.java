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

import java.util.List;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import static java.util.Collections.emptyMap;

/**
 * A {@link io.grpc.stub.StreamObserver} implementation which exposes a Kafka connector sink task through a gRPC stream.
 */
@Slf4j
public class DestinationStream implements StreamObserver<Destination.Run.Request> {
    private final SinkTask task;
    private final SchemaProvider schemaProvider;
    private final StreamObserver<Destination.Run.Response> responseObserver;

    public DestinationStream(SinkTask task,
                             SchemaProvider schemaProvider,
                             StreamObserver<Destination.Run.Response> responseObserver) {
        this.task = task;
        this.schemaProvider = schemaProvider;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(Destination.Run.Request request) {
        log.debug("Writing record...");
        try {
            // Currently, Conduit requires all writes to be asynchronous.
            // See: pkg/connector/destination.go, method Write().
            Record record = request.getRecord();
            doWrite(record);
            responseObserver.onNext(responseWith(record.getPosition()));
        } catch (Exception e) {
            log.error("Couldn't write record.", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("couldn't write record: " + e.getMessage())
                            .withCause(e)
                            .asException()
            );
        }
    }

    private Destination.Run.Response responseWith(ByteString position) {
        return Destination.Run.Response
                .newBuilder()
                .setAckPosition(position)
                .build();
    }

    private void doWrite(Record record) {
        task.put(List.of(toSinkRecord(record)));
        task.flush(emptyMap());
    }

    @SneakyThrows
    private SinkRecord toSinkRecord(Record record) {
        // todo cache the JSON object
        var schema = schemaProvider.provide(record);
        var schemaJson = Utils.jsonConv.asJsonSchema(schema);

        Object value = Transformations.toConnectData(record, schemaJson);
        return new SinkRecord(
                schema != null ? schema.name() : null,
                0,
                Schema.STRING_SCHEMA,
                record.getKey().getRawData().toStringUtf8(),
                schema,
                value,
                0
        );
    }

    @Override
    public void onError(Throwable t) {
        log.error("Experienced an error.", t);
        responseObserver.onError(
                Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        log.info("Completed.");
        responseObserver.onCompleted();
    }
}
