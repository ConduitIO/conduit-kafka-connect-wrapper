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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import plugin.GRPCStdioGrpc;

/**
 * The {@link GrpcStdio} streams logs to the host process.
 */
public class GrpcStdio extends GRPCStdioGrpc.GRPCStdioImplBase implements Logger {
    // We need a singleton, so that the same instance (exposed as a gRPC service)
    // is used by all classes for logging.
    private static final GrpcStdio INSTANCE = new GrpcStdio();

    public static GrpcStdio get() {
        return INSTANCE;
    }

    private StreamObserver<plugin.GrpcStdio.StdioData> stream;

    private GrpcStdio() {

    }

    @Override
    public void streamStdio(Empty request, StreamObserver<plugin.GrpcStdio.StdioData> responseObserver) {
        this.stream = responseObserver;
    }

    @Override
    public void info(String format, Object... args) {
        // Conduit didn't initiate the stream yet, so we simply return
        if (stream == null) {
            return;
        }
        try {
            logToStream(format, args, plugin.GrpcStdio.StdioData.Channel.STDOUT);
        } catch (Exception e) {
            // We do not want logging to break the application
        }
    }

    @Override
    public void error(String format, Object... args) {
        // Conduit didn't initiate the stream yet, so we simply return
        if (stream == null) {
            return;
        }
        try {
            logToStream(format, args, plugin.GrpcStdio.StdioData.Channel.STDERR);
        } catch (Exception e) {
            // We do not want logging to break the application
        }
    }

    private void logToStream(String format, Object[] args, plugin.GrpcStdio.StdioData.Channel channel) {
        stream.onNext(
                plugin.GrpcStdio.StdioData.newBuilder()
                        .setChannel(channel)
                        .setData(ByteString.copyFromUtf8(String.format(format, args)))
                        .build()
        );
    }
}
