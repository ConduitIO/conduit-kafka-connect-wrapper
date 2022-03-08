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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Configure.Response;
import io.conduit.grpc.Destination.Teardown;
import io.conduit.grpc.DestinationPluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.MDC;

import static io.conduit.Utils.isEmpty;
import static io.conduit.Utils.mapper;

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
            doConfigure(new HashMap<>(request.getConfigMap()));
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

    private void doConfigure(Map<String, String> config) {
        // logging
        MDC.put("pipelineId", config.remove("pipelineId"));
        MDC.put("connectorName", config.remove("connectorName"));

        this.task = taskFactory.newSinkTask(config.remove("task.class"));
        var schema = buildSchema(config.remove("schema"));
        var schemaAutoGen = Boolean.parseBoolean(config.remove("schema.autogenerate"));
        if (schema != null && schemaAutoGen) {
            throw new IllegalStateException("You should either set the schema or have it auto-generated, but not both.");
        }
        if (schemaAutoGen) {
            this.schemaProvider = new DefaultSchemaProvider();
        } else {
            this.schemaProvider = new FixedSchemaProvider(schema);
        }
        this.config = config;
    }

    @SneakyThrows
    private Schema buildSchema(String schemaString) {
        if (isEmpty(schemaString)) {
            return null;
        }
        JsonNode schemaJson = mapper.readTree(schemaString);
        return Utils.jsonConv.asConnectSchema(schemaJson);
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
