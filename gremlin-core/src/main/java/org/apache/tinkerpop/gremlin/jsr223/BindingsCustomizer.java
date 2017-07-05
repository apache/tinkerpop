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

import javax.script.Bindings;

/**
 * Provides a way to alter the bindings on a {@link GremlinScriptEngine}. Those implementing {@link GremlinScriptEngine}
 * instances need to be concerned with accounting for this {@link Customizer}. It is handled automatically by the
 * {@link DefaultGremlinScriptEngineManager}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface BindingsCustomizer extends Customizer {
    /**
     * Gets the bindings to add to a {@link GremlinScriptEngine}.
     */
    public Bindings getBindings();

    /**
     * Gets the scope to which the bindings apply. The scope is determined by the {@code ScriptContext} values where
     * "100" is {@code EngineScope} (bindings apply to the current {@link GremlinScriptEngine}) and "200" is
     * {@code GlobalScope} (bindings apply to the engines created by the current {@link GremlinScriptEngineManager}.
     */
    public int getScope();
}
