package io.conduit;

public final class Utils {
    private Utils() {
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim() == "";
    }
}
