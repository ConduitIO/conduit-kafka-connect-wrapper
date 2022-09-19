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

import com.google.protobuf.Struct;
import io.conduit.grpc.Record;

import static io.conduit.grpc.Operation.OPERATION_CREATE;
import static io.conduit.grpc.Operation.OPERATION_SNAPSHOT;
import static io.conduit.grpc.Operation.OPERATION_UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DebeziumPgSourceIT extends BasePostgresIT {
    @Override
    protected Map<String, String> configMap() {
        return Map.of(
            "wrapper.connector.class", "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname", "localhost",
            "database.port", "5432",
            "database.user", USER,
            "database.password", PASSWORD,
            "database.dbname", "meroxadb",
            "database.server.name", "test-server",
            "table.include.list", "public.employees"
        );
    }

    @Override
    protected void assertKeyOk(int index, Record rec) {
        assertNotNull(rec.getKey());
        assertTrue(rec.getKey().hasStructuredData());
        assertEquals(index, rec.getKey().getStructuredData().getFieldsOrThrow("id").getNumberValue());
    }

    @Override
    protected void assertNameUpdated(Record updated) {
        assertTrue(updated.getPayload().getAfter().hasStructuredData());
        assertTrue(updated.getPayload().getBefore().hasStructuredData());

        Struct after = updated.getPayload().getAfter().getStructuredData();
        assertEquals("foobar", after.getFieldsOrThrow("name").getStringValue());
    }

    @Override
    protected void assertSnapshotRecord(int index, Record rec) {
        assertKeyOk(index, rec);

        assertEquals(OPERATION_SNAPSHOT, rec.getOperation());

        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertFalse(rec.getPayload().getBefore().hasStructuredData());
        assertFalse(rec.getPayload().getBefore().hasRawData());

        Struct after = rec.getPayload().getAfter().getStructuredData();
        assertPayloadOk(index, after);
    }

    @Override
    protected void assertCreatedRecord(int index, Record rec) {
        assertKeyOk(index, rec);

        assertEquals(OPERATION_CREATE, rec.getOperation());

        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertFalse(rec.getPayload().getBefore().hasStructuredData());
        assertFalse(rec.getPayload().getBefore().hasRawData());

        Struct after = rec.getPayload().getAfter().getStructuredData();
        assertPayloadOk(index, after);
    }

    @Override
    protected void assertUpdateOperation(Record updated) {
        assertEquals(OPERATION_UPDATE, updated.getOperation());
    }
}
