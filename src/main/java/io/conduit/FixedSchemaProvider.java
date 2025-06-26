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

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

/**
 * A {@link SchemaProvider} which is always returning a single, provided schema.
 * The provided schema can be <code>null</code>.
 */
public class FixedSchemaProvider implements SchemaProvider {
    private Schema schema;

    public FixedSchemaProvider(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema provide(Record rec) {
        return schema;
    }

    @Override
    public String name() {
        if (schema == null) {
            return null;
        }
        return schema.name();
    }
}
