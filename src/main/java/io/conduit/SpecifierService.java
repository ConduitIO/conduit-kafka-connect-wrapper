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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Parameter.Validation;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.conduit.grpc.SpecifierPluginGrpc;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC service exposing this connector's specification.
 */
public class SpecifierService extends SpecifierPluginGrpc.SpecifierPluginImplBase {
    public static final Logger logger = LoggerFactory.getLogger(SpecifierService.class);

    private static final Validation requiredValidation = Validation.newBuilder()
        .setType(Validation.Type.TYPE_REQUIRED)
        .build();

    @Override
    public void specify(Request request, StreamObserver<Response> responseObserver) {
        responseObserver.onNext(
            Response.newBuilder()
                .setName("conduit-kafka-connect-wrapper")
                .setSummary("Kafka Connect wrapper")
                .setDescription(
                    "Conduit's Kafka Connect wrapper makes it possible "
                        + "to use existing Kafka Connect connectors with Conduit.")
                .setVersion("v0.3.0")
                .setAuthor("Meroxa, Inc.")
                .putAllSourceParams(buildSourceParams())
                .putAllDestinationParams(buildDestinationParams())
                .build()
        );
        responseObserver.onCompleted();
    }

    private Map<String, Specifier.Parameter> buildDestinationParams() {
        return Map.of(
            "wrapper.connector.class",
            Specifier.Parameter.newBuilder()
                .setDescription(
                    "The class of the requested connector. "
                        + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
                .setDefault("none")
                .addValidations(requiredValidation)
                .addValidations(availableConnectorsValidation(SinkConnector.class))
                .build(),

            "wrapper.schema",
            Specifier.Parameter.newBuilder()
                .setDescription("The schema of the records which will be written to a destination connector.")
                .setDefault("none")
                .build(),

            "wrapper.schema.autogenerate.enabled",
            Specifier.Parameter.newBuilder()
                .setDescription(
                    "Automatically generate schemas (destination connector). "
                        + "Cannot be `true` if a schema is set.")
                .setDefault("none")
                .build(),

            "wrapper.schema.autogenerate.name",
            Specifier.Parameter.newBuilder()
                .setDescription("Name of automatically generated schema. "
                    + "Required if schema auto-generation is turned on.")
                .setDefault("none")
                .build(),

            "wrapper.schema.autogenerate.overrides",
            Specifier.Parameter.newBuilder()
                .setDescription("A (partial) schema which overrides types in the auto-generated schema.")
                .setDefault("none")
                .build()
        );
    }

    private Map<String, Specifier.Parameter> buildSourceParams() {
        return Map.of(
            "wrapper.connector.class",
            Specifier.Parameter.newBuilder()
                .setDescription(
                    "The class of the requested connector. "
                        + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
                .setDefault("none")
                .addValidations(requiredValidation)
                .addValidations(availableConnectorsValidation(SourceConnector.class))
                .build()
        );
    }

    private Validation availableConnectorsValidation(Class<? extends Connector> connectorClass) {
        return Validation.newBuilder()
            .setType(Validation.Type.TYPE_INCLUSION)
            .setValue(loadAvailableConnectors(connectorClass))
            .build();
    }

    private String loadAvailableConnectors(Class<? extends Connector> connectorClass) {
        List<String> connectors = new LinkedList<>();

        ClassGraph cg = new ClassGraph()
            .verbose(false)
            .enableClassInfo()
            .acceptLibOrExtJars();

        try (ScanResult scanResult = cg.scan()) {
            ClassInfoList controlClasses = scanResult.getSubclasses(connectorClass.getName());
            for (ClassInfo cc : controlClasses) {
                if (!cc.isAbstract()) {
                    connectors.add(cc.getName());
                }
            }
        }

        logger.info("Found connectors {}", connectors);
        return String.join(",", connectors);
    }
}
