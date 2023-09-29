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

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Parameter.Validation;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.grpc.stub.StreamObserver;
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
        verifyWrapperClassValidations(
            new String[]{
                "io.aiven.connect.jdbc.JdbcSourceConnector",
                "io.debezium.connector.postgresql.PostgresConnector"
            },
            response.getSourceParamsMap()
        );

        assertNotNull(response.getDestinationParamsMap());
        assertFalse(response.getDestinationParamsMap().isEmpty());
        verifyWrapperClassValidations(
            new String[]{
                "com.snowflake.kafka.connector.SnowflakeSinkConnector",
                "io.aiven.connect.jdbc.JdbcSinkConnector"
            },
            response.getDestinationParamsMap()
        );
    }

    private void verifyWrapperClassValidations(String[] expected, Map<String, Specifier.Parameter> params) {
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
        Arrays.sort(expected);
        var actual = values.get().getValue().split(",");
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }
}