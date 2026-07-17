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
package org.apache.tinkerpop.tinkeradoc;

import java.io.IOException;
import java.util.List;

/**
 * Callback interface invoked by the {@link GremlinTreeprocessor} when the document signals
 * that the console needs to be restarted with certain plugins excluded.
 * <p>
 * The orchestration layer implements this to close the current console, toggle/rename plugin
 * directories, and start a new console instance.
 */
@FunctionalInterface
public interface ConsoleRestartHandler {

    /**
     * Called when the document attribute {@code :gremlin-docs-plugins-exclude:} is detected
     * with a changed value. The handler should close the current console, exclude the specified
     * plugin directories, and start a new console.
     *
     * @param excludedPlugins list of plugin directory names to exclude (e.g., "neo4j-gremlin")
     * @throws IOException if the restart operation fails
     */
    void onRestart(List<String> excludedPlugins) throws IOException;
}
