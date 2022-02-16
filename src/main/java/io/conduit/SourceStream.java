package io.conduit;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;
import java.util.function.Function;

/**
 * A {@link io.grpc.stub.StreamObserver} implementation which exposes a Kafka connector source task through a gRPC stream.
 */
@Slf4j
public class SourceStream implements StreamObserver<Source.Run.Request>, Runnable {
    private final SourceTask task;
    private final StreamObserver<Source.Run.Response> responseObserver;
    private final Thread thread;
    private boolean shouldRun = true;

    private final Queue<SourceRecord> buffer = new LinkedList<>();
    private final Function<SourceRecord, Record.Builder> transformer;
    private final Map<Map<String, ?>, Map<String, ?>> positions = new HashMap<>();

    public SourceStream(SourceTask task,
                        StreamObserver<Source.Run.Response> responseObserver,
                        Function<SourceRecord, Record.Builder> transformer) {
        this.task = task;
        this.responseObserver = responseObserver;
        this.transformer = transformer;
        // todo move out logic from the constructor
        this.thread = new Thread(this);
        thread.setUncaughtExceptionHandler((t, e) -> {
            log.error("Uncaught exception for thread {}.", t.getName(), e);
            onError(e);
        });
        thread.start();
    }

    @Override
    public void run() {
        while (shouldRun) {
            try {
                if (buffer.isEmpty()) {
                    fillBuffer();
                }
                SourceRecord record = buffer.poll();
                responseObserver.onNext(responseWith(record));
            } catch (Exception e) {
                log.error("Couldn't write record.", e);
                responseObserver.onError(
                        Status.INTERNAL.withDescription("couldn't read record: " + e.getMessage()).withCause(e).asException()
                );
            }
        }
        log.info("SourceStream loop stopped.");
    }

    @Override
    public void onNext(Source.Run.Request value) {
        // todo
        log.warn("Acknowledging record not implemented yet...");
    }

    @SneakyThrows
    private void fillBuffer() {
        List<SourceRecord> polled = task.poll();
        while (Utils.isEmpty(polled)) {
            polled = task.poll();
        }
        buffer.addAll(polled);
    }

    @SneakyThrows
    private Source.Run.Response responseWith(SourceRecord record) {
        positions.put(record.sourcePartition(), record.sourceOffset());
        String json = Utils.mapper.writeValueAsString(positions);
        ByteString position = ByteString.copyFromUtf8(json);
        Record.Builder conduitRec = transformer.apply(record)
                .setPosition(position);

        return Source.Run.Response.newBuilder()
                .setRecord(conduitRec)
                .build();
    }

    @Override
    public void onError(Throwable t) {
        log.error("Experienced an error.", t);
        stop();
        responseObserver.onError(
                Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        log.info("Completed.");
        stop();
        responseObserver.onCompleted();
    }

    private void stop() {
        log.info("Stopping...");
        shouldRun = false;
    }
}
