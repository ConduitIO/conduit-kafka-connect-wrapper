/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.sql.Connection;
import java.sql.DriverManager;

import lombok.SneakyThrows;

import static java.lang.Integer.parseInt;

/**
 * The plugin's entry point.
 */
public class Application {
    @SneakyThrows
    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static void main(String[] args) {
        int port = 0;
        if (args.length > 0) {
            port = parseInt(args[0]);
        }
        Server server = new Server(port);
        server.start();
        System.out.printf("1|1|tcp|localhost:%d|grpc\n", server.getPort());
        Connection c = DriverManager.getConnection("");
        c.prepareStatement("");
        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");
        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        c = DriverManager.getConnection("");
        c.prepareStatement("");

        server.blockUntilShutdown();
    }
}
