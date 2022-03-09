package io.conduit;

import io.conduit.grpc.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.connect.data.Schema;

@AllArgsConstructor
public class RawDataSchemaProvider implements SchemaProvider {
    private final String name;
    private final Schema overrides;

    @Override
    public Schema provide(Record record) {
        return null;
    }
}
