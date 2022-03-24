package io.conduit;

public interface Logger {
    void info(String format, Object... args);
    void error(String format, Object... args);
}
