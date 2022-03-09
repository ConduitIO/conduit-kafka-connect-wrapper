package io.conduit;

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public class CombinedSchemaProvider implements SchemaProvider {
    private final RawDataSchemaProvider jsonSP;
    private final StructSchemaProvider structSP;

    public CombinedSchemaProvider(String name, Schema overrides) {
        this.jsonSP = new RawDataSchemaProvider(name, overrides);
        this.structSP = new StructSchemaProvider(name, overrides);
    }

    @Override
    public Schema provide(Record record) {
        if (!record.hasPayload()) {
            return null;
        }
        if (record.getPayload().hasStructuredData()) {
            return structSP.provide(record);
        }
        return null;
    }
}
