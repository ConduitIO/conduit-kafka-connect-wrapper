package io.conduit;

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public class FixedSchemaProvider implements SchemaProvider {
    private final Schema schema;

    public FixedSchemaProvider(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema provide(Record record, String name) {
        return schema;
    }
}
