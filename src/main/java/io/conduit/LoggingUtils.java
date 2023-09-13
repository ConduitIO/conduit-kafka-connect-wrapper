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

import java.util.Map;

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
     * Configures logging levels based on the input.
     * Keys are expected to be logger names and values logging levels.
     * Possible values for logging levels are:
     * OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL.
     * Logging levels are case-insensitive.
     *
     *@param cfg configuration map
     */
    public static void configureLogging(Map<String, String> cfg) {
        if (Utils.isEmpty(cfg)) {
            return;
        }
        cfg.forEach((logger, level) -> {
            ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            logbackLogger.setLevel(Level.toLevel(level));
        });
    }
}
