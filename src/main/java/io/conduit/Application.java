package io.conduit;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Integer.parseInt;

/**
 * The plugin's entry point.
 */
@Slf4j
public class Application {
    @SneakyThrows
    public static void main(String[] args) {
        int port = 0;
        if (args.length > 0) {
            port = parseInt(args[0]);
        }
        Server server = new Server(port);
        server.start();
        System.out.printf("1|1|tcp|localhost:%d|grpc\n", server.getPort());
        server.blockUntilShutdown();
    }
}
