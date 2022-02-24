package io.conduit;

import io.conduit.grpc.Source;
import io.conduit.grpc.SourcePluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

/**
 * A gRPC service exposing source plugin methods.
 */
@Slf4j
public class SourceService extends SourcePluginGrpc.SourcePluginImplBase {
    private final TaskFactory taskFactory;
    private SourceTask task;
    private HashMap<String, String> config;
    private boolean started;
    private SourceStream runStream;
    private SourcePosition position;

    public SourceService(TaskFactory taskFactory) {
        this.taskFactory = taskFactory;
    }

    @Override
    public void configure(Source.Configure.Request request, StreamObserver<Source.Configure.Response> responseObserver) {
        log.info("Configuring the source.");

        try {
            // the returned config map is unmodifiable, so we make a copy
            // since we need to remove some keys
            doConfigure(new HashMap<>(request.getConfigMap()));
            log.info("Done configuring the source.");

            responseObserver.onNext(Source.Configure.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while configuring source.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't configure task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    private void doConfigure(HashMap<String, String> config) {
        // logging
        MDC.put("pipelineId", config.remove("pipelineId"));
        MDC.put("connectorName", config.remove("connectorName"));

        this.task = taskFactory.newSourceTask(config.remove("task.class"));
        this.config = config;
    }

    @Override
    public void start(Source.Start.Request request, StreamObserver<Source.Start.Response> responseObserver) {
        log.info("Starting the source.");

        try {
            this.position = SourcePosition.fromString(request.getPosition().toStringUtf8());
            task.initialize(
                    new SimpleSourceTaskCtx(config, position)
            );
            task.start(config);
            started = true;
            log.info("Source started.");

            responseObserver.onNext(Source.Start.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while starting.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't start task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    @Override
    public StreamObserver<Source.Run.Request> run(StreamObserver<Source.Run.Response> responseObserver) {
        this.runStream = new SourceStream(
                task,
                position,
                responseObserver,
                Transformations::fromKafkaSource
        );
        runStream.start();
        return runStream;
    }

    @Override
    public void stop(Source.Stop.Request request, StreamObserver<Source.Stop.Response> responseObserver) {
        // todo check if a record is being flushed
        runStream.onCompleted();
        responseObserver.onNext(Source.Stop.Response.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void teardown(Source.Teardown.Request request, StreamObserver<Source.Teardown.Response> responseObserver) {
        log.info("Tearing down...");
        try {
            if (task != null && started) {
                task.stop();
            }
            responseObserver.onNext(Source.Teardown.Response.newBuilder().build());
            responseObserver.onCompleted();
            log.info("Torn down.");
        } catch (Exception e) {
            log.error("Couldn't tear down.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Couldn't tear down: " + e.getMessage()).withCause(e).asException()
            );
        }
    }
}
