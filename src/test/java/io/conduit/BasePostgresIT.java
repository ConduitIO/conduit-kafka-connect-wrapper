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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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

import static io.conduit.grpc.Operation.OPERATION_DELETE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * A base test for PostgreSQL sources. It assumes a certain structure of the test table and test records.
 * See {@link BasePostgresIT#prepareTable()} for more details.
 */
abstract class BasePostgresIT {

    public static final String USER = "meroxauser";
    public static final String PASSWORD = "meroxapass";
    public static final String PG_URL = "jdbc:postgresql://localhost/meroxadb" +
            "?user=" + USER +
            "&password=" + PASSWORD +
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
        int count = 5;
        insertEmployees(1, count);

        StreamObserver runStream = TestUtils.run(underTest, configMap());
        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, timeout(1000).times(count)).onNext(captor.capture());
        verify(runStream, never()).onError(any());
        List<Source.Run.Response> responses = captor.getAllValues();
        for (int i = 0; i < count; i++) {
            Record rec = responses.get(i).getRecord();
            assertSnapshotRecord(i + 1, rec);
        }

        int updated = 3;
        insertEmployees(count + 1, count + updated);
        count += updated;

        captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, timeout(1500).times(count)).onNext(captor.capture());
        verify(runStream, never()).onError(any());
        responses = captor.getAllValues();
        // check only the three updated records
        for (int i = count - updated; i < count; i++) {
            Record rec = responses.get(i).getRecord();
            assertCreatedRecord(i + 1, rec);
        }
    }

    @SneakyThrows
    @Test
    public void testUpdatedData() {
        insertEmployees(1, 1);

        StreamObserver runStream = TestUtils.run(underTest, configMap());
        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, timeout(1000)).onNext(captor.capture());
        verify(runStream, never()).onError(any());
        assertSnapshotRecord(1, captor.getAllValues().get(0).getRecord());

        updateName(1, "foobar");
        captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        // times(2) is because all invocations of runStream are counted together
        verify(runStream, timeout(1000).times(2)).onNext(captor.capture());
        verify(runStream, never()).onError(any());

        Record updated = captor.getAllValues().get(1).getRecord();
        assertKeyOk(1, updated);
        assertUpdateOperation(updated);
        assertNameUpdated(updated);
    }

    @SneakyThrows
    @Test
    public void testDeletedData() {
        insertEmployees(1, 1);

        StreamObserver runStream = TestUtils.run(underTest, configMap());
        var captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        verify(runStream, timeout(1500)).onNext(captor.capture());
        verify(runStream, never()).onError(any());
        assertSnapshotRecord(1, captor.getAllValues().get(0).getRecord());

        delete(1);
        captor = ArgumentCaptor.forClass(Source.Run.Response.class);
        // times(2) is because all invocations of runStream are counted together
        verify(runStream, timeout(2000).times(2)).onNext(captor.capture());
        verify(runStream, never()).onError(any());

        Record deleted = captor.getAllValues().get(1).getRecord();
        assertKeyOk(1, deleted);
        assertEquals(OPERATION_DELETE, deleted.getOperation());

        assertFalse(deleted.getPayload().getAfter().hasStructuredData());
        assertFalse(deleted.getPayload().getAfter().hasRawData());
        assertTrue(deleted.getPayload().getBefore().hasStructuredData());
    }

    @SneakyThrows
    private void prepareTable() {
        PreparedStatement ps = conn.prepareStatement(
                Files.readString(Path.of("src/test/resources/pg-prepare.sql"))
        );
        ps.execute();
    }

    /**
     * Inserts test data, information about employees, into the source. Assumes that ID is auto-generated.
     */
    @SneakyThrows
    private void insertEmployees(int from, int to) {
        String sql = "INSERT INTO employees (name,full_time,updated_at) VALUES ";
        List<String> placeholders = new LinkedList<>();
        for (int i = from; i <= to; i++) {
            placeholders.add("(?,?,?)");
        }
        sql += String.join(",", placeholders);
        sql += ";";

        try (var ps = conn.prepareStatement(sql);) {
            int index = 1;
            for (int i = from; i <= to; i++) {
                ps.setString(index++, "name " + i);
                ps.setBoolean(index++, i % 2 == 0);
                ps.setObject(index++, OffsetDateTime.now(ZoneOffset.UTC));
            }
            ps.execute();
        }
    }

    @SneakyThrows
    private void updateName(int id, String name) {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE employees SET name = ?, updated_at = ? WHERE id = ?;")) {
            ps.setString(1, name);
            ps.setObject(2, OffsetDateTime.now(ZoneOffset.UTC));
            ps.setInt(3, id);
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

    protected void assertPayloadOk(int index, Struct payload) {
        assertEquals(index, payload.getFieldsOrThrow("id").getNumberValue());
        assertEquals("name " + index, payload.getFieldsOrThrow("name").getStringValue());
        assertEquals(index % 2 == 0, payload.getFieldsOrThrow("full_time").getBoolValue());
    }

    protected abstract void assertNameUpdated(Record updated);

    protected abstract void assertSnapshotRecord(int index, Record rec);

    protected abstract void assertCreatedRecord(int index, Record rec);

    protected abstract Map<String, String> configMap();

    protected abstract void assertKeyOk(int index, Record rec);

    protected abstract void assertUpdateOperation(Record updated);
}
