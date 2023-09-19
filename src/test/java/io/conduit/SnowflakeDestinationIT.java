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

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Opencdc;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SnowflakeDestinationIT {
    private static final ObjectMapper mapper = new ObjectMapper();
    private DestinationService underTest;

    @BeforeEach
    public void setUp() {
        underTest = new DestinationService(new ClasspathTaskFactory());
        cleanTable();
    }

    @AfterEach
    public void tearDown() {
        cleanTable();
    }

    @SneakyThrows
    private void cleanTable() {
        try (var conn = getConnection();
             var stmt = conn.prepareStatement("delete from CUSTOMERS_TEST")) {
            stmt.execute();
        }
    }

    // todo it's probably better to just fail the test, so it doesn't get silently ignored
    @Test
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_USER_NAME", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_URL_NAME", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_PRIVATE_KEY", matches = ".*")
    public void test() {
        var cfgStream = mock(StreamObserver.class);
        underTest.configure(makeCfgReq(), cfgStream);
        verify(cfgStream, never()).onError(any());

        var startStream = mock(StreamObserver.class);
        underTest.start(Destination.Start.Request.newBuilder().build(), startStream);
        verify(startStream, never()).onError(any());

        var respStream = mock(StreamObserver.class);
        StreamObserver reqStream = underTest.run(respStream);
        List<Record> records = buildTestRecords();
        records.forEach(rec ->
            reqStream.onNext(Destination.Run.Request.newBuilder().setRecord(rec).build())
        );


        verify(respStream, never()).onError(any());
        verify(respStream, times(records.size())).onNext(any());
        // We need to check them all at once, so speed up the test.
        // Checking them individually is better for test reports, but very slow.
        assertWritten(records);
    }

    @SneakyThrows
    private void assertWritten(List<Record> records) {
        List<Record> missingRecords = new LinkedList<>(records);

        try (var conn = getConnection()) {
            ResultSet rs = waitForFirstRow(conn);

            // Go over data in destination and remove from list of records we expect to be found.
            do {
                String content = rs.getString("RECORD_CONTENT");
                String metadata = rs.getString("RECORD_METADATA");
                assertTrue(
                    remove(missingRecords, content, metadata),
                    "got unexpected row: " + content
                );
            } while (rs.next());

            rs.close();
        }

        assertTrue(missingRecords.isEmpty());
    }

    // Wait until data in destination table is available.
    // If rows are detected, the result set cursor is moved to the first row.
    private ResultSet waitForFirstRow(Connection conn) throws SQLException, InterruptedException {
        try (var stmt = conn.prepareStatement("select * from CUSTOMERS_TEST")) {
            long waitUntil = currentTimeMillis() + 30_000;
            ResultSet rs = null;
            while (currentTimeMillis() < waitUntil) {
                assertTrue(stmt.execute());
                rs = stmt.getResultSet();
                if (rs.next()) {
                    break;
                }
                Thread.sleep(1000);
            }
            return rs;
        }
    }

    @SneakyThrows
    private Connection getConnection() {
        var cfgMap = cfgMap();

        String url = "jdbc:snowflake://" + cfgMap.get("snowflake.url.name");
        Properties prop = new Properties();
        prop.put("user", cfgMap.get("snowflake.user.name"));
        prop.put("privateKey", getPrivateKey());
        prop.put("db", cfgMap.get("snowflake.database.name"));
        prop.put("schema", cfgMap.get("snowflake.schema.name"));
        prop.put("warehouse", "COMPUTE_WH");
        prop.put("role", "SYSADMIN");

        return DriverManager.getConnection(url, prop);
    }

    @SneakyThrows
    private PrivateKey getPrivateKey() {
        String pkcs8Pem = cfgMap().get("snowflake.private.key");
        pkcs8Pem = pkcs8Pem.replace("-----BEGIN PRIVATE KEY-----", "");
        pkcs8Pem = pkcs8Pem.replace("-----END PRIVATE KEY-----", "");
        pkcs8Pem = pkcs8Pem.replaceAll("\\s+", "");

        byte[] bytes = Base64.getDecoder().decode(pkcs8Pem);

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    private Map<String, String> cfgMap() {
        Map<String, String> map = new HashMap<>();
        map.put("wrapper.connector.class", "com.snowflake.kafka.connector.SnowflakeSinkConnector");
        map.put("wrapper.schema.autogenerate.enabled", "true");
        map.put("wrapper.schema.autogenerate.name", "CUSTOMERS_TEST");
        map.put("tasks.max", "1");
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

    @SneakyThrows
    // Removes a record from the list of records, which has the given content and key (taken from the metadata string)
    // Returns `true` if a record was found, `false` otherwise.
    private boolean remove(List<Record> records, String content, String metadata) {
        Iterator<Record> it = records.iterator();
        while (it.hasNext()) {
            Record rec = it.next();
            if (matches(content, metadata, rec)) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    // Checks if given records has the provided content and key (taken from the metadata string)
    private boolean matches(String content, String metadata, Record rec) throws JsonProcessingException {
        if (rec.getPayload().getAfter().hasRawData()) {
            return matchesRawJson(content, metadata, rec);
        }
        if (rec.getPayload().getAfter().hasStructuredData()) {
            return matchesStruct(content, metadata, rec);
        }
        throw new IllegalArgumentException("record without any payload");
    }

    @SneakyThrows
    private boolean matchesStruct(String content, String metadata, Record rec) {
        var key = rec.getKey().getRawData().toStringUtf8();
        var payloadStruct = rec.getPayload().getAfter().getStructuredData();

        var contentStruct = Struct.newBuilder();
        JsonFormat.parser().merge(content, contentStruct);

        var metaJson = mapper.readTree(metadata);
        return payloadStruct.equals(contentStruct.build()) && metaJson.path("key").asText().equals(key);
    }

    @SneakyThrows
    private boolean matchesRawJson(String content, String metadata, Record rec) {
        var key = rec.getKey().getRawData().toStringUtf8();
        var payloadJson = mapper.readTree(rec.getPayload().getAfter().getRawData().toStringUtf8());
        var contentJson = mapper.readTree(content);
        var metaJson = mapper.readTree(metadata);
        return payloadJson.equals(contentJson) && metaJson.path("key").asText().equals(key);
    }

    // Builds a list of test records.
    // Each record has a different combination of a key, payload, position and created-at timestamp.
    private List<Record> buildTestRecords() {
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
                .setPayload((Change) input.get(1).get())
                .setPosition((ByteString) input.get(2).get())
                .putMetadata(Opencdc.metadataCreatedAt.getDefaultValue(), (String) input.get(3).get())
                .build()
            ).collect(Collectors.toList());
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

    private static Set<Supplier<Change>> payloadGenerators() {
        return Stream.of(
            Data.newBuilder().setRawData(ByteString.copyFromUtf8("{\"id\":123,\"name\":\"foobar\"}")).build(),
            Data.newBuilder().setRawData(ByteString.copyFromUtf8("{}")).build(),
            Data.newBuilder()
                .setStructuredData(Struct.newBuilder()
                    .putFields("id", Value.newBuilder().setNumberValue(123).build())
                    .putFields("name", Value.newBuilder().setStringValue("foobar").build())
                    .build()
                ).build()
        ).map(d -> {
            Supplier<Change> s = () -> Change.newBuilder().setAfter(d).build();
            return s;
        }).collect(Collectors.toSet());
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
}
