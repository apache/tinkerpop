/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.slf4j.Logger;

import java.util.Map;

/**
 * Provides a method in which users can hook into the startup and shutdown lifecycle of Gremlin Server.  Creating
 * an instance of this interface in a Gremlin Server initialization script enables the ability to get a callback
 * when the server starts and stops.  The hook is guaranteed to execute once at in both cases.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface LifeCycleHook {

    /**
     * Called once after instantiation to pass configuration from the {@code lifecycleHooks} YAML section.
     * Implementations that require configuration should override this method.  The default implementation
     * is a no-op so that existing hooks are not forced to implement it.
     *
     * <p>If this method throws an exception, the hook is skipped and the server continues to start without it.
     *
     * @param config the key/value pairs from the {@code config} block in the YAML entry
     */
    public default void init(final Map<String, Object> config) {}

    /**
     * Called when the server starts up.  The graph collection will have been initialized at this point
     * and all initialization scripts will have been executed when this callback is called.
     *
     * <p>If this method throws an exception (other than {@link UnsupportedOperationException}), the server
     * will fail to start.
     */
    public void onStartUp(final Context c);

    /**
     * Called when the server is shutdown.
     *
     * <p>If this method throws an exception, remaining shutdown hooks and graph cleanup may be skipped.
     */
    public void onShutDown(final Context c);

    /**
     * Contains objects from the server that might be useful to scripting during the startup and shutdown process.
     */
    public static class Context {
        private final Logger logger;
        private final GraphManager graphManager;

        /**
         * @deprecated As of release 4.0.0, replaced by {@link #Context(Logger, GraphManager)}.
         */
        @Deprecated
        public Context(final Logger logger) {
            this(logger, null);
        }

        public Context(final Logger logger, final GraphManager graphManager) {
            this.logger = logger;
            this.graphManager = graphManager;
        }

        public Logger getLogger() {
            return logger;
        }

        /**
         * Gets the {@link GraphManager} which provides access to all configured graphs and traversal sources.
         */
        public GraphManager getGraphManager() {
            return graphManager;
        }
    }
}
