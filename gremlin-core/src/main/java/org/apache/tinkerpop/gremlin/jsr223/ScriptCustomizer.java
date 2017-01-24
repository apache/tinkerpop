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
package org.apache.tinkerpop.gremlin.jsr223;

import javax.script.ScriptEngineManager;
import java.util.Collection;
import java.util.List;

/**
 * A {@link Customizer} that executes scripts in a {@link GremlinScriptEngine} instance for purpose of initialization.
 * Implementors of a {@link GremlinScriptEngine} do not need to be concerned with supporting this {@link Customizer}.
 * This is work for the {@link ScriptEngineManager} implementation since scripts typically require access to global
 * bindings and those are not applied to the {@link GremlinScriptEngine} until after construction.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ScriptCustomizer extends Customizer {
    /**
     * Gets a collection of scripts where each is represented as a list of script lines.
     */
    public Collection<List<String>> getScripts();
}
