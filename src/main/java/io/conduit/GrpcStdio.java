package io.conduit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import plugin.GRPCStdioGrpc;

public class GrpcStdio extends GRPCStdioGrpc.GRPCStdioImplBase implements Logger {
    private StreamObserver<plugin.GrpcStdio.StdioData> stream;

    @Override
    public void streamStdio(Empty request, StreamObserver<plugin.GrpcStdio.StdioData> responseObserver) {
        this.stream = responseObserver;
    }

    @Override
    public void info(String format, Object... args) {
        if (stream == null) {
            return;
        }
        stream.onNext(
                plugin.GrpcStdio.StdioData.newBuilder()
                        .setChannel(plugin.GrpcStdio.StdioData.Channel.STDOUT)
                        .setData(ByteString.copyFromUtf8(String.format(format, args)))
                        .build()
        );
    }

    @Override
    public void error(String format, Object... args) {
        if (stream == null) {
            return;
        }
        stream.onNext(
                plugin.GrpcStdio.StdioData.newBuilder()
                        .setChannel(plugin.GrpcStdio.StdioData.Channel.STDERR)
                        .setData(ByteString.copyFromUtf8(String.format(format, args)))
                        .build()
        );
    }
}
