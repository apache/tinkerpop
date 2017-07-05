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
import javax.script.ScriptContext;

/**
 * Default implementation of the {@link BindingsCustomizer} which adds bindings to the {@code ScriptContext.GLOBAL_SCOPE}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultBindingsCustomizer implements BindingsCustomizer {

    private final Bindings bindings;
    private final int scope;

    /**
     * Creates a new object with {@code ScriptContext.GLOBAL_SCOPE}.
     */
    public DefaultBindingsCustomizer(final Bindings bindings) {
        this(bindings, ScriptContext.GLOBAL_SCOPE);
    }

    /**
     * Creates a new object with a specified scope. There really can't be anything other than a {@code GLOBAL_SCOPE}
     * specification so this constructor isn't public at the moment. Assigning to {@code ENGINE_SCOPE} is useless
     * because it is the nature of the {@code ScriptEngine} to override that scope with {@code Bindings} supplied at
     * the time of execution.
     */
    DefaultBindingsCustomizer(final Bindings bindings, final int scope) {
        this.bindings = bindings;
        this.scope = scope;
    }

    @Override
    public Bindings getBindings() {
        return bindings;
    }

    @Override
    public int getScope() {
        return scope;
    }
}
