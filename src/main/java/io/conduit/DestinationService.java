package io.conduit;

import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Configure.Response;
import io.conduit.grpc.DestinationPluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class DestinationService extends DestinationPluginGrpc.DestinationPluginImplBase {
    @Override
    public void configure(Destination.Configure.Request request, StreamObserver<Response> responseObserver) {
        log.info("Configuring the destination.");

        try {
            doConfigure(request.getConfigMap());
            log.info("Done configuring the destination.");

            responseObserver.onNext(Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while opening destination.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't start task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    private void doConfigure(Map<String, String> config) {
        // todo implement me
    }

    @Override
    public void start(Destination.Start.Request request, StreamObserver<Destination.Start.Response> responseObserver) {
        super.start(request, responseObserver);
    }

    @Override
    public StreamObserver<Destination.Run.Request> run(StreamObserver<Destination.Run.Response> responseObserver) {
        return super.run(responseObserver);
    }

    @Override
    public void stop(Destination.Stop.Request request, StreamObserver<Destination.Stop.Response> responseObserver) {
        super.stop(request, responseObserver);
    }

    @Override
    public void teardown(Destination.Teardown.Request request, StreamObserver<Destination.Teardown.Response> responseObserver) {
        super.teardown(request, responseObserver);
    }
}
