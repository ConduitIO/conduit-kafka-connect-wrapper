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

import java.util.Map;

import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Configure.Response;
import io.conduit.grpc.Destination.Teardown;
import io.conduit.grpc.DestinationPluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.MDC;

/**
 * A gRPC service exposing source plugin methods.
 */
@Slf4j
public class DestinationService extends DestinationPluginGrpc.DestinationPluginImplBase {
    private final TaskFactory taskFactory;
    private SinkTask task;
    private Map<String, String> config;
    private DestinationStream runStream;
    private boolean started;
    private SchemaProvider schemaProvider;

    public DestinationService(TaskFactory taskFactory) {
        this.taskFactory = taskFactory;
    }

    @Override
    public void configure(Destination.Configure.Request request, StreamObserver<Response> responseObserver) {
        log.info("Configuring the destination.");

        try {
            // the returned config map is unmodifiable, so we make a copy
            // since we need to remove some keys
            doConfigure(DestinationConfig.fromMap(request.getConfigMap()));
            log.info("Done configuring the destination.");

            responseObserver.onNext(Destination.Configure.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while configuring destination.", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("couldn't configure task: " + e.getMessage())
                            .withCause(e)
                            .asException()
            );
        }
    }

    private void doConfigure(DestinationConfig config) {
        // logging
        MDC.put("pipelineId", config.getPipelineId());
        MDC.put("connectorName", config.getConnectorName());

        this.task = taskFactory.newSinkTask(config.getTaskClass());
        this.schemaProvider = buildSchemaProvider(config);
        this.config = config.getKafkaConnectorCfg();
    }

    private SchemaProvider buildSchemaProvider(DestinationConfig config) {
        if (config.hasSchema() && config.isSchemaAutogenerate()) {
            throw new IllegalArgumentException(
                    "You cannot provide a schema and use schema auto-generation at the same time."
            );
        }

        if (config.hasSchema()) {
            return new FixedSchemaProvider(config.getSchema());
        }

        if (config.isSchemaAutogenerate()) {
            String name = config.getSchemaName();
            if (name == null) {
                throw new IllegalArgumentException("Schema name not provided");
            }

            return new CombinedSchemaProvider(name, config.getOverrides());
        }
        // No schema information provided, so no schema is to be used.
        return new FixedSchemaProvider(null);
    }

    @Override
    public void start(Destination.Start.Request request, StreamObserver<Destination.Start.Response> responseObserver) {
        log.info("Starting the destination.");

        try {
            task.start(config);
            started = true;
            log.info("Destination started.");

            responseObserver.onNext(Destination.Start.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while starting.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't start task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    @Override
    public StreamObserver<Destination.Run.Request> run(StreamObserver<Destination.Run.Response> responseObserver) {
        this.runStream = new DestinationStream(task, schemaProvider, responseObserver);
        return runStream;
    }

    @Override
    public void stop(Destination.Stop.Request request, StreamObserver<Destination.Stop.Response> responseObserver) {
        // todo check if a record is being flushed
        runStream.onCompleted();
        responseObserver.onNext(Destination.Stop.Response.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void teardown(Teardown.Request request, StreamObserver<Teardown.Response> responseObserver) {
        log.info("Tearing down...");
        try {
            if (task != null && started) {
                task.stop();
            }
            responseObserver.onNext(Teardown.Response.newBuilder().build());
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
