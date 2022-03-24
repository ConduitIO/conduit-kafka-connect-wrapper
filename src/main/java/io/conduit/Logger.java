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

/**
 * Internal logger interface. The input for all logging methods is formatted
 * using {@link String#format(String, Object...)}.
 */
public interface Logger {
    static Logger get() {
        return GrpcStdio.get();
    }

    void info(String format, Object... args);

    void error(String format, Object... args);
}
