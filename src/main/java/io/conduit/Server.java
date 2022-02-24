package io.conduit;

import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Server {
    private final io.grpc.Server server;

    public Server(int port) {
        this(ServerBuilder.forPort(port));
    }

    public Server(ServerBuilder<?> serverBuilder) {
        ClasspathTaskFactory taskFactory = new ClasspathTaskFactory();
        server = serverBuilder
                .addService(new DestinationService(taskFactory))
                .addService(new SourceService(taskFactory))
                .build();
    }

    public void start() throws IOException {
        log.info("Starting server...");
        server.start();
        log.info("Started on port {}", getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Server.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public int getPort() {
        return server.getPort();
    }
}
