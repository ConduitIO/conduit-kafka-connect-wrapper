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
 * Contains OpenCDC metadata constants.
 */
public final class OpenCdcMetadata {
    private OpenCdcMetadata() {

    }
    /**
     * CreatedAt can contain the time when the record was created in the 3rd party system.
     * The expected format is a unix timestamp in nanoseconds.
     */
    public static String CREATED_AT = "opencdc.createdAt";
}
