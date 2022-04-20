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

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;

public class SnowflakeDestinationIT {
    private DestinationService underTest;

    @BeforeEach
    public void setUp() {
        underTest = new DestinationService(new ClasspathTaskFactory());
    }

    // todo it's probably better to just fail the test, so it doesn't get silently ignored
    @ParameterizedTest
    @MethodSource("buildTestRecords")
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_USER_NAME", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_URL_NAME", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_PRIVATE_KEY", matches = ".*")
    void test(Record rec) {
        var cfgStream = mock(StreamObserver.class);
        underTest.configure(makeCfgReq(), cfgStream);
        verify(cfgStream, never()).onError(any());

        var startStream = mock(StreamObserver.class);
        underTest.start(Destination.Start.Request.newBuilder().build(), startStream);
        verify(startStream, never()).onError(any());

        var respStream = mock(StreamObserver.class);
        StreamObserver reqStream = underTest.run(respStream);
        reqStream.onNext(Destination.Run.Request.newBuilder().setRecord(rec).build());

        verify(respStream, never()).onError(any());
    }

    private static Stream<Record> buildTestRecords() {
        // Combinations of different types of keys, payloads etc.
        Set<List<Supplier>> combinations = Sets.cartesianProduct(
                keyGenerators(),
                payloadGenerators(),
                positionGenerators(),
                timestampGenerators()
        );

        return combinations.stream()
                .map(input -> Record.newBuilder()
                        .setKey((Data) input.get(0).get())
                        .setPayload((Data) input.get(1).get())
                        .setPosition((ByteString) input.get(2).get())
                        .build()
                );
    }

    private static Set<Supplier<Timestamp>> timestampGenerators() {
        return Set.of(
                () -> Timestamp.newBuilder().build(),
                () -> Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build()
        );
    }

    private static Set<Supplier<ByteString>> positionGenerators() {
        return Set.of(
                () -> ByteString.EMPTY,
                () -> ByteString.copyFromUtf8(randomUUID().toString())
        );
    }

    private static Set<Supplier<Data>> payloadGenerators() {
        return Set.of(
                () -> Data.newBuilder().setRawData(ByteString.copyFromUtf8("{\"id\":123,\"name\":\"foobar\"}")).build(),
                () -> Data.newBuilder().setRawData(ByteString.copyFromUtf8("{}")).build()
                // todo check if this can be worked around
                // () -> Data.newBuilder().setRawData(ByteString.copyFromUtf8("raw payload")).build(),
                // () -> Data.newBuilder().build()
        );
    }

    private static Set<Supplier<Data>> keyGenerators() {
        return Set.of(
                () -> Data.newBuilder().setRawData(ByteString.copyFromUtf8("key-" + currentTimeMillis())).build(),
                () -> Data.newBuilder().build()
        );
    }

    private Destination.Configure.Request makeCfgReq() {
        return Destination.Configure.Request.newBuilder()
                .putAllConfig(cfgMap())
                .build();
    }

    private Map<String, String> cfgMap() {
        Map<String, String> map = new HashMap<>();
        map.put("wrapper.connector.class", "com.snowflake.kafka.connector.SnowflakeSinkConnector");
        map.put("wrapper.schema.autogenerate.enabled", "true");
        map.put("wrapper.schema.autogenerate.name", "CUSTOMERS_TEST");
        map.put("tasks.max", "8");
        map.put("topics", "customers");
        map.put("name", "mysnowflakesink");
        map.put("snowflake.topic2table.map", "customers:CUSTOMERS_TEST");
        map.put("buffer.count.records", "1");
        map.put("buffer.flush.time", "0");
        map.put("buffer.size.bytes", "1");
        map.put("input.data.format", "JSON");
        map.put("snowflake.url.name", System.getenv("SNOWFLAKE_URL_NAME"));
        map.put("snowflake.user.name", System.getenv("SNOWFLAKE_USER_NAME"));
        map.put("snowflake.private.key", System.getenv("SNOWFLAKE_PRIVATE_KEY"));
        map.put("snowflake.database.name", "CONDUIT_TEST_DB");
        map.put("snowflake.schema.name", "STREAM_DATA");
        map.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        map.put("value.converter", "com.snowflake.kafka.connector.records.SnowflakeJsonConverter");
        return map;
    }
}
