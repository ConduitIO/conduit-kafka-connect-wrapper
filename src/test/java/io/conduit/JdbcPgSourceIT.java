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

import io.conduit.grpc.Record;

import static io.conduit.grpc.Operation.OPERATION_CREATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcPgSourceIT extends BasePostgresIT {
    @Override
    public void testDeletedData() {
        // todo implement this
        // more setup is needed to make the JDBC connector detect deleted rows
    }

    @Override
    protected Map<String, String> configMap() {
        return Map.of(
            "wrapper.connector.class", "io.aiven.connect.jdbc.JdbcSourceConnector",
            "connection.url", PG_URL,
            "connection.user", USER,
            "connection.password", PASSWORD,
            "tables", "employees",
            "mode", "timestamp",
            "poll.interval.ms", "500",
            "timestamp.column.name", "updated_at",
            "topic.prefix", "my_topic_prefix"
        );
    }

    @Override
    protected void assertNameUpdated(Record updated) {
        assertTrue(updated.getPayload().getAfter().hasStructuredData());
        assertEquals(
            "foobar",
            updated.getPayload().getAfter().getStructuredData().getFieldsOrThrow("name").getStringValue()
        );
    }

    @Override
    protected void assertSnapshotRecord(int index, Record rec) {
        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertPayloadOk(index, rec.getPayload().getAfter().getStructuredData());
    }

    @Override
    protected void assertCreatedRecord(int index, Record rec) {
        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        assertPayloadOk(index, rec.getPayload().getAfter().getStructuredData());
    }

    @Override
    protected void assertKeyOk(int index, Record rec) {
        assertEquals(index, rec.getPayload().getAfter().getStructuredData().getFieldsOrThrow("id").getNumberValue());
    }

    @Override
    protected void assertUpdateOperation(Record updated) {
        assertEquals(OPERATION_CREATE, updated.getOperation());
    }
}
