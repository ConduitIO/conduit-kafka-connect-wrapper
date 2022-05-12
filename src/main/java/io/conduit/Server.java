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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.grpc.ServerBuilder;

/**
 * A wrapper around {@link io.grpc.Server}, which also adds shutdown hooks.
 */
public class Server {
    private final io.grpc.Server grpcServer;

    public Server(int port) {
        this(ServerBuilder.forPort(port));
    }

    public Server(ServerBuilder<?> serverBuilder) {
        ClasspathTaskFactory taskFactory = new ClasspathTaskFactory();
        grpcServer = serverBuilder
                .addService(new DestinationService(taskFactory))
                .addService(new SourceService(taskFactory))
                .addService(GrpcStdio.get())
                .build();
    }

    /**
     * Starts this server on the configured port. Also adds a JVM shutdown hook,
     * so that the server is shut down, when the JVM shutdown is triggered.
     *
     * @throws IOException if the server cannot be started
     */
    public void start() throws IOException {
        grpcServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Server.this.stop();
            } catch (InterruptedException e) { //NOSONAR no need to re-throw, as this is a shutdown hook
                // Re-throwing the InterruptedException or interrupting the thread here
                // doesn't make much of a difference.
                // The exception would be handled by the uncaught exception handler,
                // which would, by default, do the same thing.
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    /**
     * Initiates an orderly shutdown of the server and waits at most 30 seconds
     * for the server to terminate (i.e. no running calls).
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void stop() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits for the server to become terminated.
     *
     * @throws InterruptedException if interrupted while blocking
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public int getPort() {
        return grpcServer.getPort();
    }
}
