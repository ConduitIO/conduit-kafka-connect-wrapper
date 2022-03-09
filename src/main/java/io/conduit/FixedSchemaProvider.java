package io.conduit;

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public class FixedSchemaProvider implements SchemaProvider {
    private Schema schema;

    public FixedSchemaProvider(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema provide(Record record) {
        return schema;
    }
}
