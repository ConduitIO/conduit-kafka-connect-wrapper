package io.conduit;

import lombok.SneakyThrows;

public class Application {
    @SneakyThrows
    public static void main(String[] args) {
        Server server = new Server(0);
        server.start();
        System.out.printf("1|1|tcp|localhost:%d|grpc\n", server.getPort());
        server.blockUntilShutdown();
    }
}
