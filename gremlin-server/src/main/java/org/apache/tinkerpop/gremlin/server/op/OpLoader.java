/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.op;

import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Uses {@link ServiceLoader} to load {@link OpProcessor} instances into a cache.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class OpLoader {
    private static final Logger logger = LoggerFactory.getLogger(OpLoader.class);

    private static final Map<String, OpProcessor> processors = new HashMap<>();

    static {
        Map<String, OpProcessor> replacements = new HashMap<>();

        ServiceLoader.load(OpProcessor.class).forEach(op -> {
            final String name = op.getName();
            final String loggingName = calculateLoggingName(name);
            final Optional<String> replacedOpProcessor = op.replacedOpProcessorName();

            replacedOpProcessor.ifPresent(replacedOpProcessorName -> {
                String replacedLoggingName = calculateLoggingName(replacedOpProcessorName);
                if (replacements.containsKey(replacedOpProcessorName)) {
                    throw new RuntimeException(String.format(
                            "There is a naming conflict. OpProcessor %s with name %s is designed to replace " +
                                    "OpProcessor with name %s, but replacement " +
                                    "of this OpProcessor already registered.", op.getClass().getCanonicalName(), loggingName,
                            replacedLoggingName));
                }
                replacements.put(replacedOpProcessorName, op);
            });

            if (!replacedOpProcessor.isPresent()) {
                logger.info("Adding the {} OpProcessor.", loggingName);
                if (processors.containsKey(name)) {
                    throw new RuntimeException(String.format("There is a naming conflict with the %s OpProcessor implementations.", loggingName));
                }

                processors.put(name, op);
            }
        });

        replacements.forEach((replacedOpProcessorName, opProcessor) -> {
            final String replacedLoggingName = calculateLoggingName(replacedOpProcessorName);
            if (processors.containsKey(replacedOpProcessorName)) {
                final String name = opProcessor.getName();
                final String loggingName = calculateLoggingName(name);
                OpProcessor oldProcessor = processors.remove(replacedOpProcessorName);
                try {
                    oldProcessor.close();
                } catch (Exception ex) {
                    logger.warn("Exception thrown while closing the {} OpProcessor with name {}.",
                            oldProcessor.getClass().getName(), replacedLoggingName, ex);
                }

                if (processors.containsKey(name)) {
                    throw new RuntimeException(String.format("There is a naming conflict with the %s OpProcessor implementations.", loggingName));
                }

                logger.info("Replacing the {} OpProcessor with name {} with the {} OpProcessor with name {}.",
                        oldProcessor.getClass().getName(), replacedLoggingName, opProcessor.getClass().getName(), loggingName);
                processors.put(name, opProcessor);
            } else {
                logger.warn("Attempted to replace the OpProcessor with name {} with the {} OpProcessor with name {}, " +
                                "but replacing OpProcessor is not registered.",
                        replacedLoggingName, opProcessor.getClass().getName(), opProcessor.getName());
            }
        });
    }

    private static String calculateLoggingName(String name) {
        return name.equals(StandardOpProcessor.OP_PROCESSOR_NAME) ? "standard" : name;
    }

    private static volatile boolean initialized = false;

    /**
     * Initialize the {@code OpLoader} with server settings. This method should only be called once at startup but is
     * designed to be idempotent.
     */
    public static synchronized void init(final Settings settings) {
        if (!initialized) {
            processors.values().forEach(processor -> processor.init(settings));
            initialized = true;
        }
    }

    /**
     * Gets an {@link OpProcessor} by its name. If it cannot be found an {@link Optional#empty()} is returned.
     */
    public static Optional<OpProcessor> getProcessor(final String name) {
        return Optional.ofNullable(processors.get(name));
    }

    /**
     * Gets a read-only map of the processors where the key is the {@link OpProcessor} name and the value is the
     * instance created by {@link ServiceLoader}.
     */
    public static Map<String, OpProcessor> getProcessors() {
        return Collections.unmodifiableMap(processors);
    }

    /**
     * Reset the processors so that they can be re-initialized with different settings which is useful in testing
     * scenarios.
     */
    public synchronized static void reset() {
        initialized = false;
    }
}
