package io.conduit;

import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

@Slf4j
public class SourceStream implements StreamObserver<Source.Run.Request>, Runnable {
    private final SourceTask task;
    private final StreamObserver<Source.Run.Response> responseObserver;
    private Function<SourceRecord, Record> transformer;
    private final Queue<SourceRecord> buffer = new LinkedList<>();
    private final Thread thread;
    private boolean shouldRun = true;

    public SourceStream(SourceTask task,
                        StreamObserver<Source.Run.Response> responseObserver,
                        Function<SourceRecord, Record> transformer) {
        this.task = task;
        this.responseObserver = responseObserver;
        this.transformer = transformer;
        // todo catch errors from thread
        this.thread = new Thread(this);
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

    private Source.Run.Response responseWith(SourceRecord record) {
        return Source.Run.Response.newBuilder()
                .setRecord(transformer.apply(record))
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
        shouldRun = false;
    }
}
