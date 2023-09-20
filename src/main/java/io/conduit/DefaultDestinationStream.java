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
import java.util.Map;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link io.grpc.stub.StreamObserver} implementation which exposes a Kafka connector sink task through a gRPC stream.
 */
public class DefaultDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(DefaultDestinationStream.class);

    private final SinkTask task;
    private final SchemaProvider schemaProvider;
    private final StreamObserver<Destination.Run.Response> responseObserver;
    private final ToConnectData toConnectData;

    public DefaultDestinationStream(SinkTask task,
                                    SchemaProvider schemaProvider,
                                    StreamObserver<Destination.Run.Response> responseObserver) {
        this.task = task;
        this.schemaProvider = schemaProvider;
        this.responseObserver = responseObserver;
        this.toConnectData = new ToConnectData();
    }

    @Override
    public void onNext(Destination.Run.Request request) {
        try {
            // Currently, Conduit requires all writes to be asynchronous.
            // See: pkg/connector/destination.go, method Write().
            Record rec = request.getRecord();
            doWrite(rec);
            responseObserver.onNext(responseWith(rec.getPosition()));
        } catch (Exception e) {
            logger.error("Couldn't write record.", e);
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

    private void doWrite(Record rec) {
        SinkRecord sinkRecord = toSinkRecord(rec);
        task.put(List.of(sinkRecord));
        task.preCommit(Map.of(
            new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition()),
            new OffsetAndMetadata(sinkRecord.kafkaOffset())
        ));
    }

    @SneakyThrows
    private SinkRecord toSinkRecord(Record rec) {
        // todo cache the JSON object
        // Also related to: https://github.com/ConduitIO/conduit-kafka-connect-wrapper/issues/58
        var schema = schemaProvider.provide(rec);

        Object value = toConnectData.apply(rec, schema);
        var schemaUsed = getSchema(value, schema);
        // While there's no real topic involved, we still assign values
        // to topic, partition and offset since the underlying connector might use them.
        // The offset is set to System.currentTimeMillis() to mimic the increasing
        // offset values if a Kafka topic partition.
        return new SinkRecord(
            schemaUsed != null ? schemaUsed.name() : null,
            0,
            Schema.STRING_SCHEMA,
            rec.getKey().getRawData().toStringUtf8(),
            schemaUsed,
            value,
            System.currentTimeMillis()
        );
    }

    private Schema getSchema(Object value, Schema schema) {
        // Context: Kafka structs have a reference to the schema they use.
        // In addition to that, SinkRecords also contain a schema (see method: toSinkRecord()).
        // Some connectors check that the struct's schema is the same as the declared schema in the SinkRecord.
        // Some do so by checking the equality of references and not objects.
        if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        return schema;
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Experienced an error.", t);
        responseObserver.onError(
            Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        logger.info("Completed.");
        responseObserver.onCompleted();
    }
}
