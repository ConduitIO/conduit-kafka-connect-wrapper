package io.conduit;

import io.conduit.grpc.Record;
import org.apache.kafka.connect.data.Schema;

public interface SchemaProvider {
    Schema provide(Record record, String name);
}
