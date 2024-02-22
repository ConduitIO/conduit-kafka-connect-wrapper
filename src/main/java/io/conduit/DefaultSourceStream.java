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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link io.grpc.stub.StreamObserver} implementation which exposes a Kafka connector source task
 * through a gRPC stream.
 */
public class DefaultSourceStream implements SourceStream {
    public static final Logger logger = LoggerFactory.getLogger(DefaultSourceStream.class);

    private final SourceTask task;
    private final StreamObserver<Source.Run.Response> responseObserver;
    private boolean shouldRun = true;

    private final Function<SourceRecord, Record.Builder> transformer;
    private final SourcePosition position;
    // readRecords is a queue where we save records which have been read
    // from the underlying Kafka Connect source.
    // We need the SourceRecord (so we can commit it),
    // and the Conduit record (so we can find the position to be acked)
    private final Queue<Pair<SourceRecord, Record>> readRecords = new ConcurrentLinkedQueue<>();

    public DefaultSourceStream(SourceTask task,
                               SourcePosition position,
                               StreamObserver<Source.Run.Response> responseObserver,
                               Function<SourceRecord, Record.Builder> transformer) {
        this.task = task;
        this.position = position;
        this.responseObserver = responseObserver;
        this.transformer = transformer;
    }

    @Override
    public void run() {
        while (shouldRun) {
            try {
                getRecords();
            } catch (Exception e) {
                logger.error("Couldn't write record.", e);
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription("couldn't read record: " + e.getMessage())
                        .withCause(e)
                        .asException()
                );
            }
        }
        logger.info("SourceStream loop stopped.");
    }

    /**
     * Gets records from the underlying source task (via task.poll())
     * converts them to Conduit records, and sends them to the gRPC stream.
     **/
    public void getRecords() {
        var polled = getKafkaSourceRecords();
        polled.forEach(rec -> {
            // We may get so-called tombstone records, i.e. records with a null payload.
            // This can happen when records are deleted, for example.
            // This is used in Kafka Connect internally, more precisely for log compaction in Kafka.
            // For more info: https://kafka.apache.org/documentation/#compaction
            if (rec.value() != null) {
                Source.Run.Response resp = responseWith(rec);
                readRecords.add(new Pair<>(rec, resp.getRecord()));

                responseObserver.onNext(resp);
            }
        });
    }

    @Override
    @SneakyThrows
    public void onNext(Source.Run.Request request) {
        var pair = readRecords.poll();

        if (!request.getAckPosition().equals(pair.right.getPosition())) {
            responseObserver.onError(new IllegalArgumentException(
                String.format("position %s not found in read records", request.getAckPosition().toStringUtf8())
            ));

            return;
        }

        task.commitRecord(pair.left, null);
        task.commit();

    }

    @SneakyThrows
    private List<SourceRecord> getKafkaSourceRecords() {
        List<SourceRecord> polled = task.poll();
        while (Utils.isEmpty(polled)) {
            polled = task.poll();
        }

        return polled;
    }

    @SneakyThrows
    private Source.Run.Response responseWith(SourceRecord rec) {
        position.add(rec.sourcePartition(), rec.sourceOffset());

        Record.Builder conduitRec = transformer.apply(rec)
            .setPosition(position.asByteString());

        return Source.Run.Response.newBuilder()
            .setRecord(conduitRec)
            .build();
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Experienced an error.", t);
        stop();
        responseObserver.onError(
            Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        logger.info("Completed.");
        stop();
        responseObserver.onCompleted();
    }

    private void stop() {
        logger.info("Stopping...");
        shouldRun = false;
    }

    /**
     * Starts this stream. The method is not blocking -- the actual work is done in a separate thread.
     */
    public void startAsync() {
        Thread thread = new Thread(this);
        thread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Uncaught exception for thread {}.", t.getName(), e);
            onError(e);
        });
        thread.start();
    }

    @Override
    public ByteString lastRead() {
        return position.asByteString();
    }
}
