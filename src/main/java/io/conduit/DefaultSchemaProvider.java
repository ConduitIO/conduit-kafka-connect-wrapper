package io.conduit;

import java.util.Map;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public class DefaultSchemaProvider implements SchemaProvider {
    private final String name;

    public DefaultSchemaProvider(String name) {
        this.name = name;
    }

    @Override
    public Schema provide(Record record) {
        if (!record.hasPayload()) {
            return null;
        }
        if (record.getPayload().hasStructuredData()) {
            return provideForStructured(record);
        }
        return null;
    }

    private Schema provideForStructured(Record record) {
        Struct struct = record.getPayload().getStructuredData();
        Map<String, Value> fieldsMap = struct.getFieldsMap();
        return null;
    }
}
