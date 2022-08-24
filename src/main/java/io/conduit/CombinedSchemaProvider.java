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
 * A {@link SchemaProvider} implementation which combines the functionality of
 * a {@link RawDataSchemaProvider} and a {@link StructSchemaProvider}.
 */
public class CombinedSchemaProvider implements SchemaProvider {
    private final RawDataSchemaProvider rawDataSP;
    private final StructSchemaProvider structSP;

    public CombinedSchemaProvider(String name, Schema overrides) {
        this.rawDataSP = new RawDataSchemaProvider(name, overrides);
        this.structSP = new StructSchemaProvider(name, overrides);
    }

    @Override
    public Schema provide(Record rec) {
        if (!rec.hasPayload() || !rec.getPayload().hasAfter()) {
            return null;
        }
        if (rec.getPayload().getAfter().hasStructuredData()) {
            return structSP.provide(rec);
        }
        if (rec.getPayload().getAfter().hasRawData()) {
            return rawDataSP.provide(rec);
        }
        return null;
    }
}
