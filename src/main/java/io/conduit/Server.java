package io.conduit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * A wrapper around {@link io.grpc.Server}, which also adds shutdown hooks.
 */
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

    /**
     * Starts this server on the configured port. Also adds a JVM shutdown hook,
     * so that the server is shut down, when the JVM shutdown is triggered.
     *
     *@throws IOException if the server cannot be started
     */
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

    /**
     * Initiates an orderly shutdown of the server and waits at most 30 seconds
     * for the server to terminate (i.e. no running calls).
     *
     *@throws InterruptedException if interrupted while waiting
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits for the server to become terminated.
     *
     *@throws InterruptedException if interrupted while blocking
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public int getPort() {
        return server.getPort();
    }
}
