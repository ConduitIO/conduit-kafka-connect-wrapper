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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Struct;
import io.conduit.grpc.Record;
import io.conduit.grpc.Source;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SourceServiceIT {

    public static final String PG_URL = "jdbc:postgresql://localhost/meroxadb" +
            "?user=meroxauser" +
            "&password=meroxapass" +
            "&sslmode=disable" +
            "&allowMultiQueries=true";
    private Connection conn;
    private SourceService underTest;

    @BeforeEach
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(PG_URL);
        prepareTable();
        underTest = new SourceService(new ClasspathTaskFactory());
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
        underTest.stop(Source.Stop.Request.newBuilder().build(), mock(StreamObserver.class));
        underTest.teardown(Source.Teardown.Request.newBuilder().build(), mock(StreamObserver.class));
    }

    @SneakyThrows
    @Test
    public void testReadExistingAndNewData() {
        int existing = 5;
        insertEmployees(1, existing);

        StreamObserver runStream = run();
        Thread.sleep(1500);

        int updated = 3;
        insertEmployees(existing + 1, existing + updated);
        Thread.sleep(2000);

        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        int count = existing + updated;
        verify(runStream, never()).onError(any());
        verify(runStream, times(count)).onNext(captor.capture());
        List<Source.Run.Response> responses = captor.getAllValues();
        for (int i = 0; i < count; i++) {
            Record rec = responses.get(i).getRecord();
            assertNewRecordOk(i + 1, rec);
        }
    }

    @SneakyThrows
    @Test
    public void testUpdatedData() {
        int existing = 1;
        insertEmployees(1, existing);

        StreamObserver runStream = run();
        Thread.sleep(500);

        updateName(1, "foobar");
        Thread.sleep(1500);

        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, never()).onError(any());
        verify(runStream, times(2)).onNext(captor.capture());
        List<Source.Run.Response> responses = captor.getAllValues();

        assertNewRecordOk(1, responses.get(0).getRecord());

        Record updated = responses.get(1).getRecord();
        assertNotNull(updated.getKey());
        assertTrue(updated.getPayload().hasStructuredData());
        Struct struct = updated.getPayload().getStructuredData();
        assertTrue(struct.getFieldsOrThrow("source").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("before").hasNullValue());
        assertTrue(struct.getFieldsOrThrow("after").hasStructValue());

        Struct after = struct.getFieldsOrThrow("after").getStructValue();
        assertEquals("foobar", after.getFieldsOrThrow("name").getStringValue());
    }

    @SneakyThrows
    @Test
    public void testDeletedData() {
        int existing = 1;
        insertEmployees(1, existing);

        StreamObserver runStream = run();
        Thread.sleep(500);

        delete(1);
        Thread.sleep(1500);

        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, never()).onError(any());
        verify(runStream, times(2)).onNext(captor.capture());
        List<Source.Run.Response> responses = captor.getAllValues();

        assertNewRecordOk(1, responses.get(0).getRecord());

        Record updated = responses.get(1).getRecord();
        assertNotNull(updated.getKey());
        assertTrue(updated.getPayload().hasStructuredData());
        Struct struct = updated.getPayload().getStructuredData();
        assertTrue(struct.getFieldsOrThrow("source").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("before").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("after").hasNullValue());
    }

    private void assertNewRecordOk(int index, Record rec) {
        // todo
        assertNotNull(rec.getKey());
        assertTrue(rec.getPayload().hasStructuredData());
        Struct struct = rec.getPayload().getStructuredData();
        assertTrue(struct.getFieldsOrThrow("source").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("before").hasNullValue());
        assertTrue(struct.getFieldsOrThrow("after").hasStructValue());

        Struct after = struct.getFieldsOrThrow("after").getStructValue();
        assertStructOk(index, after);
    }

    private StreamObserver run() {
        StreamObserver cfgStream = mock(StreamObserver.class);
        underTest.configure(
                TestUtils.newConfigRequest(Map.of(
                        "wrapper.connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                        "database.hostname", "localhost",
                        "database.port", "5432",
                        "database.user", "meroxauser",
                        "database.password", "meroxapass",
                        "database.dbname", "meroxadb",
                        "database.server.name", "test-server",
                        "table.include.list", "public.employees"
                )),
                cfgStream
        );
        verify(cfgStream, never()).onError(any());

        StreamObserver startStream = mock(StreamObserver.class);
        underTest.start(
                Source.Start.Request.newBuilder().build(),
                startStream
        );
        verify(startStream, never()).onError(any());

        StreamObserver runStream = mock(StreamObserver.class);
        underTest.run(runStream);

        return runStream;
    }

    @SneakyThrows
    private void prepareTable() {
        PreparedStatement ps = conn.prepareStatement(
                "DROP TABLE IF EXISTS employees;\n" +
                        "DROP SEQUENCE IF EXISTS employees_id_seq;\n" +
                        "CREATE TABLE employees (\n" +
                        "    id int,\n" +
                        "    name varchar(255),\n" +
                        "    full_time bool,\n" +
                        "    PRIMARY KEY (id)\n" +
                        ");\n" +
                        "\n" +
                        "CREATE SEQUENCE employees_id_seq;\n" +
                        "ALTER TABLE employees ALTER id SET DEFAULT NEXTVAL('employees_id_seq');"
        );
        ps.execute();
    }

    /**
     * Inserts test data, information about employees, into the source. Assumes that ID is auto-generated.
     */
    @SneakyThrows
    private void insertEmployees(int from, int to) {
        String sql = "INSERT INTO employees (name,full_time) VALUES ";
        List<String> placeholders = new LinkedList<>();
        for (int i = from; i <= to; i++) {
            placeholders.add("(?,?)");
        }
        sql += String.join(",", placeholders);
        sql += ";";

        try (var ps = conn.prepareStatement(sql);) {
            int index = 1;
            for (int i = from; i <= to; i++) {
                ps.setString(index++, "name " + i);
                ps.setBoolean(index++, i % 2 == 0);
            }
            ps.execute();
        }
    }

    @SneakyThrows
    private void updateName(int id, String name) {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE employees SET name = ? WHERE id = ?;")) {
            ps.setString(1, name);
            ps.setInt(2, id);
            ps.execute();
        }
    }

    @SneakyThrows
    private void delete(int id) {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM employees WHERE id = ?;")) {
            ps.setInt(1, id);
            ps.execute();
        }
    }

    private void assertStructOk(int index, Struct after) {
        assertEquals(index, after.getFieldsOrThrow("id").getNumberValue());
        assertEquals("name " + index, after.getFieldsOrThrow("name").getStringValue());
        assertEquals(index % 2 == 0, after.getFieldsOrThrow("full_time").getBoolValue());
    }
}
