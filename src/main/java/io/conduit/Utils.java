package io.conduit;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.Collection;
import java.util.Map;

public final class Utils {
    public static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    /**
     * A Kafka JsonConverter which includes schemas in serialized data
     * (and also expects the schema when deserializing data).
     */
    public static final JsonConverter jsonConv = new JsonConverter();

    static {
        jsonConv.configure(Map.of(
                "schemas.enable", true,
                "converter.type", "value"
        ));
    }

    /**
     * A Kafka JsonConverter which does NOT include schemas in serialized data
     * (and also doesn't expect the schema when deserializing data).
     */
    public static final JsonConverter jsonConvSchemaless = new JsonConverter();

    static {
        jsonConvSchemaless.configure(Map.of(
                "schemas.enable", false,
                "converter.type", "value"
        ));
    }

    private Utils() {
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isBlank();
    }

    public static <K, V> boolean isEmpty(Map<K, V> map) {
        return map == null || map.isEmpty();
    }

    public static <T> boolean isEmpty(Collection<T> coll) {
        return coll == null || coll.isEmpty();
    }
}
