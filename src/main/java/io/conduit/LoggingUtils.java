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

import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of utility methods for logging.
 */
public final class LoggingUtils {
    private LoggingUtils() {
    }

    /**
     * Sets the root logging level.
     * Possible values are: <code>OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL</code>.
     * Logging levels are case-insensitive.
     *
     * @param level root logging level
     */
    public static void setLevel(String level) {
        if (Utils.isEmpty(level)) {
            return;
        }

        // Logback only at the moment, log4j TBD
        ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger)
            LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logbackLogger.setLevel(Level.toLevel(level));
    }
}
