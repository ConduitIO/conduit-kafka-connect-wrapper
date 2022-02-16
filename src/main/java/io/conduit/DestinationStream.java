package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.List;

import static io.conduit.Utils.jsonConv;
import static io.conduit.Utils.mapper;
import static java.util.Collections.emptyMap;
import static java.util.UUID.randomUUID;

@Slf4j
public class DestinationStream implements StreamObserver<Destination.Run.Request> {
    private final SinkTask task;
    private final Schema schema;
    // cached JSON object
    private final ObjectNode schemaJson;
    private final StreamObserver<Destination.Run.Response> responseObserver;

    public DestinationStream(SinkTask task, Schema schema, StreamObserver<Destination.Run.Response> responseObserver) {
        this.task = task;
        this.schema = schema;
        this.schemaJson = Utils.jsonConv.asJsonSchema(schema);
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(Destination.Run.Request request) {
        log.debug("Writing record...");
        try {
            // Currently, Conduit requires all writes to be asynchronous.
            // See: pkg/connector/destination.go, method Write().
            Record record = request.getRecord();
            doWrite(record);
            responseObserver.onNext(responseWith(record.getPosition()));
        } catch (Exception e) {
            log.error("Couldn't write record.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't write record: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    private Destination.Run.Response responseWith(ByteString position) {
        return Destination.Run.Response
                .newBuilder()
                .setAckPosition(position)
                .build();
    }

    private void doWrite(Record record) {
        task.put(List.of(toSinkRecord(record)));
        task.flush(emptyMap());
    }

    @SneakyThrows
    private SinkRecord toSinkRecord(Record record) {
        // todo optimize memory usage here
        // for each record, we're creating a new JSON object
        // and copying data into it.
        // something as simple as concatenating strings could work.
        JsonNode payloadJson = Utils.mapper.readTree(
                record.getPayload().getRawData().toByteArray()
        );

        ObjectNode json = Utils.mapper.createObjectNode();
        json.set("schema", schemaJson);
        json.set("payload", payloadJson);

        byte[] bytes = Utils.mapper.writeValueAsBytes(json);
        // topic arg unused in the connect-json library
        Struct struct = (Struct) Utils.jsonConv.toConnectData("", bytes).value();

        return new SinkRecord(
                schema.name(),
                0,
                Schema.STRING_SCHEMA,
                randomUUID().toString(),
                schema,
                struct,
                0
        );
    }

    @Override
    public void onError(Throwable t) {
        log.error("Experienced an error.", t);
        responseObserver.onError(
                Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        log.info("Completed.");
        responseObserver.onCompleted();
    }
}
