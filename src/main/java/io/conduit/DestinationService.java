package io.conduit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Configure.Response;
import io.conduit.grpc.DestinationPluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class DestinationService extends DestinationPluginGrpc.DestinationPluginImplBase {
    private SinkTask task;
    private Schema schema;
    private int bufferSize;

    @Override
    public void configure(Destination.Configure.Request request, StreamObserver<Response> responseObserver) {
        log.info("Configuring the destination.");

        try {
            doConfigure(request.getConfigMap());
            log.info("Done configuring the destination.");

            responseObserver.onNext(Destination.Configure.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while opening destination.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't start task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    private void doConfigure(Map<String, String> config) {
        // logging
        MDC.put("pipelineId", config.remove("pipelineId"));
        MDC.put("destName", config.remove("destName"));

        this.task = newTask(config.remove("task.class"));
        this.schema = buildSchema(config.remove("schema"));
        setBufferSize(config.get("batch.size"));
    }

    @SneakyThrows
    private SinkTask newTask(String className) {
        Class<?> clazz = Class.forName(className);
        Object taskObj = Arrays.stream(clazz.getConstructors())
                .filter(c -> c.getParameterCount() == 0)
                .findFirst()
                .get()
                .newInstance();
        return (SinkTask) taskObj;
    }

    @SneakyThrows
    private Schema buildSchema(String schemaString) {
        // todo consider replacing with Gson, which is already on the classpath, to reduce the uber-jar size
        JsonNode schemaJson = new ObjectMapper().readTree(schemaString);
        final SchemaBuilder[] schema = {SchemaBuilder.struct().name(schemaJson.get("name").asText()).optional()};
        JsonNode fields = schemaJson.get("fields");
        fields.fieldNames().forEachRemaining(field -> {
            String typeString = fields.get(field).asText();
            SchemaBuilder type = new SchemaBuilder(Schema.Type.valueOf(typeString))
                    //todo make configurable
                    .optional();
            schema[0] = schema[0].field(field, type);
        });

        return schema[0];
    }

    private void setBufferSize(String buffSizeStr) {
        int buffSize;
        if (Utils.isEmpty(buffSizeStr)) {
            // not the same default as in JdbcSinkTask, which is 3000
            buffSize = 100;
        } else {
            buffSize = Integer.parseInt(buffSizeStr);
        }
        this.bufferSize = buffSize;
    }

    @Override
    public void start(Destination.Start.Request request, StreamObserver<Destination.Start.Response> responseObserver) {
        super.start(request, responseObserver);
    }

    @Override
    public StreamObserver<Destination.Run.Request> run(StreamObserver<Destination.Run.Response> responseObserver) {
        return super.run(responseObserver);
    }

    @Override
    public void stop(Destination.Stop.Request request, StreamObserver<Destination.Stop.Response> responseObserver) {
        log.info("Stopping the destination...");

        try {
            task.flush(Map.of());
            task.stop();
            log.info("Destination stopped.");

            responseObserver.onNext(Destination.Stop.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error while stopping the destination.", e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("couldn't start task: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    @Override
    public void teardown(Destination.Teardown.Request request, StreamObserver<Destination.Teardown.Response> responseObserver) {
        super.teardown(request, responseObserver);
    }
}
