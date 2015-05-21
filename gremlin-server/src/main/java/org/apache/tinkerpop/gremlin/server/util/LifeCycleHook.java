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
package org.apache.tinkerpop.gremlin.server.util;

import org.slf4j.Logger;

/**
 * Provides a method in which users can hook into the startup and shutdown lifecycle of Gremlin Server.  Creating
 * an instance of this interface in a Gremlin Server initialization script enables the ability to get a callback
 * when the server starts and stops.  The hook is guaranteed to execute once at in both cases.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface LifeCycleHook {

    /**
     * Called when the server starts up.  The graph collection will have been initialized at this point
     * and all initialization scripts will have been executed when this callback is called.
     */
    public void onStartUp(final Context c);

    /**
     * Called when the server is shutdown.
     */
    public void onShutDown(final Context c);

    /**
     * Contains objects from the server that might be useful to scripting during the startup and shutdown process.
     */
    public static class Context {
        private final Logger logger;

        public Context(final Logger logger) {
            this.logger = logger;
        }

        public Logger getLogger() {
            return logger;
        }
    }
}
