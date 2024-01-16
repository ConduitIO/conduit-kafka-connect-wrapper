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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
import lombok.SneakyThrows;
import org.apache.kafka.common.config.ConfigDef;
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

    private Map<String, Specifier.Parameter> buildSourceParams() {
        Map<String, Specifier.Parameter> params = new HashMap<>();
        params.put("wrapper.connector.class", Specifier.Parameter.newBuilder()
            .setDescription(
                "The class of the requested connector. "
                    + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
            .setDefault("none")
            .addValidations(requiredValidation)
            .addValidations(availableConnectorsValidation(SourceConnector.class))
            .build());
        params.putAll(underlyingSourceParams());
        return params;
    }

    private Map<String, Specifier.Parameter> buildDestinationParams() {
        Map<String, Specifier.Parameter> params = new HashMap<>();
        params.put("wrapper.connector.class", Specifier.Parameter.newBuilder()
            .setDescription(
                "The class of the requested connector. "
                    + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
            .setDefault("none")
            .addValidations(requiredValidation)
            .addValidations(availableConnectorsValidation(SinkConnector.class))
            .build());
        params.put("wrapper.schema", Specifier.Parameter.newBuilder()
            .setDescription("The schema of the records which will be written to a destination connector.")
            .setDefault("none")
            .build());
        params.put("wrapper.schema.autogenerate.enabled", Specifier.Parameter.newBuilder()
            .setDescription(
                "Automatically generate schemas (destination connector). "
                    + "Cannot be `true` if a schema is set.")
            .setDefault("none")
            .build());
        params.put("wrapper.schema.autogenerate.name", Specifier.Parameter.newBuilder()
            .setDescription("Name of automatically generated schema. "
                + "Required if schema auto-generation is turned on.")
            .setDefault("none")
            .build());
        params.put("wrapper.schema.autogenerate.overrides", Specifier.Parameter.newBuilder()
            .setDescription("A (partial) schema which overrides types in the auto-generated schema.")
            .setDefault("none")
            .build());
        params.putAll(underlyingSinkParams());
        return params;
    }

    private Validation availableConnectorsValidation(Class<? extends Connector> connectorClass) {
        return Validation.newBuilder()
            .setType(Validation.Type.TYPE_INCLUSION)
            .setValue(String.join(",", loadAvailableConnectors(connectorClass)))
            .build();
    }

    private List<String> loadAvailableConnectors(Class<? extends Connector> connectorClass) {
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
        return connectors;
    }

    @SneakyThrows
    private Map<String, Specifier.Parameter> underlyingSinkParams() {
        Map<String, Specifier.Parameter> params = new HashMap<>();
        for (String clazz : loadAvailableConnectors(SinkConnector.class)) {
            SinkConnector connector = (SinkConnector) newInstance(clazz);
            connector.config()
                .configKeys()
                .forEach((keyName, key) -> params.put(clazz + "." + keyName, toConduitParam(key)));
        }

        return params;
    }

    @SneakyThrows
    private Map<String, Specifier.Parameter> underlyingSourceParams() {
        Map<String, Specifier.Parameter> params = new HashMap<>();
        for (String clazz : loadAvailableConnectors(SourceConnector.class)) {
            SourceConnector connector = (SourceConnector) newInstance(clazz);
            connector.config()
                .configKeys()
                .forEach((keyName, key) -> params.put(clazz + "." + keyName, toConduitParam(key)));
        }

        return params;
    }

    private Specifier.Parameter toConduitParam(ConfigDef.ConfigKey key) {
        return Specifier.Parameter.newBuilder()
            .setDefault(paramDefaultValue(key))
            .setDescription(key.documentation)
            .setType(toConduitType(key.type))
            .addAllValidations(makeConduitValidations(key))
            .build();
    }

    private String paramDefaultValue(ConfigDef.ConfigKey key) {
        if (ConfigDef.NO_DEFAULT_VALUE.equals(key.defaultValue)) {
            return "";
        } else {
            return String.valueOf(key.defaultValue);
        }
    }

    private List<Validation> makeConduitValidations(ConfigDef.ConfigKey key) {
        List<Validation> validations = new LinkedList<>();
        if (ConfigDef.NO_DEFAULT_VALUE.equals(key.defaultValue)) {
            validations.add(Validation.newBuilder()
                .setType(Validation.Type.TYPE_REQUIRED)
                .build());
        }
        return validations;
    }

    private Specifier.Parameter.Type toConduitType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN -> {
                return Specifier.Parameter.Type.TYPE_BOOL;
            }
            case STRING, PASSWORD, CLASS, LIST -> {
                return Specifier.Parameter.Type.TYPE_STRING;
            }
            case INT, LONG, SHORT -> {
                return Specifier.Parameter.Type.TYPE_INT;
            }
            case DOUBLE -> {
                return Specifier.Parameter.Type.TYPE_FLOAT;
            }
            default -> throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    // copied from TaskFactory
    @SneakyThrows
    private Object newInstance(String className) {
        Class<?> clazz = Class.forName(className);
        return Arrays.stream(clazz.getConstructors())
            .filter(c -> c.getParameterCount() == 0)
            .findFirst()
            // get() also throws NoSuchElementException, but here:
            // 1. we send the caller more details
            // 2. we also adhere to a best practice (not calling get() without checking if value is present)
            .orElseThrow(() -> new NoSuchElementException("no parameterless constructor for " + className))
            .newInstance();
    }
}
