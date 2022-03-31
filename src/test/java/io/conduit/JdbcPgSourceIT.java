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

import static org.junit.jupiter.api.Assertions.*;

public class JdbcPgSourceIT extends BasePostgresIT {
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
        assertTrue(updated.getPayload().hasStructuredData());
        assertEquals(
                "foobar",
                updated.getPayload().getStructuredData().getFieldsOrThrow("name").getStringValue()
        );
    }

    @Override
    protected void assertNewRecordOk(int index, Record rec) {
        assertTrue(rec.getPayload().hasStructuredData());
        assertPayloadOk(index, rec.getPayload().getStructuredData());
    }

    @Override
    protected void assertKeyOk(int index, Record rec) {
        assertEquals(index, rec.getPayload().getStructuredData().getFieldsOrThrow("id").getNumberValue());
    }
}
