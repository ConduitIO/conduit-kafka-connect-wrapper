package io.conduit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.Map;

public final class Utils {
    public static final ObjectMapper mapper = new ObjectMapper();
    public static final JsonConverter jsonConv = new JsonConverter();

    static {
        jsonConv.configure(Map.of(
                "schemas.enable", true,
                "converter.type", "value"
        ));
    }

    private Utils() {
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isBlank();
    }
}
