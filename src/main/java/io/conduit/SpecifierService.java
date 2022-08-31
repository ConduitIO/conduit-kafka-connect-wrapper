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

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.conduit.grpc.SpecifierPluginGrpc;
import io.grpc.stub.StreamObserver;

/**
 * A gRPC service exposing this connector's specification.
 */
public class SpecifierService extends SpecifierPluginGrpc.SpecifierPluginImplBase {
    private static final Map<String, Specifier.Parameter> SOURCE_PARAMS = Map.of(
            "wrapper.connector.class",
            Specifier.Parameter.newBuilder()
                    .setDescription(
                            "The class of the requested connector. "
                            + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
                    .setDefault("none")
                    .setRequired(true)
                    .build()
    );

    private static final Map<String, Specifier.Parameter> DESTINATION_PARAMS = Map.of(
            "wrapper.connector.class",
            Specifier.Parameter.newBuilder()
                    .setDescription(
                            "The class of the requested connector. "
                            + "It needs to be found on the classpath, i.e. in a JAR in the `libs` directory. ")
                    .setDefault("none")
                    .setRequired(true)
                    .build(),

            "wrapper.schema",
            Specifier.Parameter.newBuilder()
                    .setDescription("The schema of the records which will be written to a destination connector.")
                    .setDefault("none")
                    .setRequired(false)
                    .build(),

            "wrapper.schema.autogenerate.enabled",
            Specifier.Parameter.newBuilder()
                    .setDescription(
                            "Automatically generate schemas (destination connector). "
                            + "Cannot be `true` if a schema is set.")
                    .setDefault("none")
                    .setRequired(false)
                    .build(),

            "wrapper.schema.autogenerate.name",
            Specifier.Parameter.newBuilder()
                    .setDescription("Name of automatically generated schema. "
                            + "Required if schema auto-generation is turned on.")
                    .setDefault("none")
                    .setRequired(false)
                    .build(),

            "wrapper.schema.autogenerate.overrides",
            Specifier.Parameter.newBuilder()
                    .setDescription("A (partial) schema which overrides types in the auto-generated schema.")
                    .setDefault("none")
                    .setRequired(false)
                    .build()
    );

    @Override
    public void specify(Request request, StreamObserver<Response> responseObserver) {
        responseObserver.onNext(
                Response.newBuilder()
                        .setName("conduit-kafka-connect-wrapper")
                        .setSummary("Kafka Connect wrapper")
                        .setDescription(
                                "Conduit's Kafka connector wrapper is to make it possible "
                                + "to use existing Kafka connectors with Conduit.")
                        .setVersion("0.2.0")
                        .setAuthor("Meroxa, Inc.")
                        .putAllSourceParams(SOURCE_PARAMS)
                        .putAllDestinationParams(DESTINATION_PARAMS)
                        .build()
        );
        responseObserver.onCompleted();
    }
}
