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
import io.conduit.grpc.Source;
import io.grpc.stub.StreamObserver;

/**
 * A <code>SourceStream</code> represents a stream of records from a Conduit source.
 */
public interface SourceStream extends StreamObserver<Source.Run.Request>, Runnable {
    /**
     * Start this stream asynchronously.
     */
    void startAsync();

    /**
     * Returns the position of the last read by this source.
     */
    ByteString lastRead();
}
