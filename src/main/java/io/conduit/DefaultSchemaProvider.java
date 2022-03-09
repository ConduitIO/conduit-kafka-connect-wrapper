package io.conduit;

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public class DefaultSchemaProvider implements SchemaProvider {
    private final String name;

    public DefaultSchemaProvider(String name) {
        this.name = name;
    }

    @Override
    public Schema provide(Record record) {
        return null;
    }
}
