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
import java.util.Map;
import java.util.NoSuchElementException;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Parameter.Validation;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SpecifierServiceTest {
    @Test
    void testSpecify() {
        var observer = mock(StreamObserver.class);

        new SpecifierService().specify(
            Request.newBuilder().build(),
            observer
        );
        verify(observer, never()).onError(any());

        var captor = ArgumentCaptor.forClass(Response.class);
        verify(observer).onNext(captor.capture());

        var response = captor.getValue();
        assertEquals("Meroxa, Inc.", response.getAuthor());

        assertNotNull(response.getDescription());
        assertFalse(response.getDescription().isBlank());

        assertNotNull(response.getName());
        assertFalse(response.getName().isBlank());

        assertNotNull(response.getSummary());
        assertFalse(response.getSummary().isBlank());

        assertNotNull(response.getVersion());
        assertTrue(response.getVersion().startsWith("v"));

        assertNotNull(response.getSourceParamsMap());
        assertFalse(response.getSourceParamsMap().isEmpty());
        verifyWrapperClass(
            new String[]{
                "io.aiven.connect.jdbc.JdbcSourceConnector",
                "io.debezium.connector.postgresql.PostgresConnector"
            },
            response.getSourceParamsMap()
        );
        verifySourceParams(
            "io.aiven.connect.jdbc.JdbcSourceConnector",
            response.getSourceParamsMap()
        );
        verifySourceParams(
            "io.debezium.connector.postgresql.PostgresConnector",
            response.getSourceParamsMap()
        );

        assertNotNull(response.getDestinationParamsMap());
        assertFalse(response.getDestinationParamsMap().isEmpty());
        verifyWrapperClass(
            new String[]{
                "io.aiven.connect.jdbc.JdbcSinkConnector",
                "com.snowflake.kafka.connector.SnowflakeSinkConnector"
            },
            response.getDestinationParamsMap()
        );
        verifyDestinationParams(
            "io.aiven.connect.jdbc.JdbcSinkConnector",
            response.getDestinationParamsMap()
        );
        verifyDestinationParams(
            "com.snowflake.kafka.connector.SnowflakeSinkConnector",
            response.getDestinationParamsMap()
        );
    }

    @SneakyThrows
    private void verifySourceParams(String clazz, Map<String, Specifier.Parameter> paramsMap) {
        SourceConnector connector = (SourceConnector) newInstance(clazz);
        connector.config()
            .configKeys()
            .forEach((keyName, key) -> verifyParam(clazz, key, paramsMap));
    }

    @SneakyThrows
    private void verifyDestinationParams(String clazz, Map<String, Specifier.Parameter> paramsMap) {
        SinkConnector connector = (SinkConnector) newInstance(clazz);
        connector.config()
            .configKeys()
            .forEach((keyName, key) -> verifyParam(clazz, key, paramsMap));
    }

    private void verifyParam(String clazz, ConfigDef.ConfigKey key, Map<String, Specifier.Parameter> paramsMap) {
        assertTrue(
            paramsMap.containsKey(clazz + "." + key.name),
            "expected params to contain: " + key.name
        );
        Specifier.Parameter param = paramsMap.get(clazz + "." + key.name);
        assertEquals(key.documentation, param.getDescription());
    }

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

    private void verifyWrapperClass(String[] expected, Map<String, Specifier.Parameter> params) {
        var validations = params.get("wrapper.connector.class").getValidationsList();
        assertNotNull(validations);
        assertEquals(2, validations.size());
        var required = validations.stream()
            .filter(v -> v.getType() == Validation.Type.TYPE_REQUIRED)
            .findFirst();
        assertTrue(required.isPresent());


        var values = validations.stream()
            .filter(v -> v.getType() == Validation.Type.TYPE_INCLUSION)
            .findFirst();
        assertTrue(values.isPresent());
        var actual = values.get().getValue().split(",");
        Arrays.sort(actual);
        Arrays.sort(expected);
        assertArrayEquals(expected, actual);
    }
}