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
import lombok.Builder;

import static org.junit.jupiter.api.Assertions.*;

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
        Struct struct = updated.getPayload().getAfter().getStructuredData();
        assertTrue(struct.getFieldsOrThrow("source").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("before").hasNullValue());
        assertTrue(struct.getFieldsOrThrow("after").hasStructValue());

        Struct after = struct.getFieldsOrThrow("after").getStructValue();
        assertEquals("foobar", after.getFieldsOrThrow("name").getStringValue());
    }

    @Override
    protected void assertNewRecordOk(int index, Record rec) {
        assertKeyOk(index, rec);

        assertTrue(rec.getPayload().getAfter().hasStructuredData());
        Struct struct = rec.getPayload().getAfter().getStructuredData();
        assertTrue(struct.getFieldsOrThrow("source").hasStructValue());
        assertTrue(struct.getFieldsOrThrow("before").hasNullValue());
        assertTrue(struct.getFieldsOrThrow("after").hasStructValue());

        Struct after = struct.getFieldsOrThrow("after").getStructValue();
        assertPayloadOk(index, after);
    }
}
