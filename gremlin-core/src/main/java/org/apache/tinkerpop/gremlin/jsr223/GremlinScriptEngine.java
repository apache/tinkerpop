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

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * A {@code GremlinScriptEngine} is an extension of the standard {@code ScriptEngine} and provides some specific
 * methods that are important to the TinkerPop environment.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinScriptEngine extends ScriptEngine {
    public static final String HIDDEN_G = "gremlinscriptengine__g";

    @Override
    public GremlinScriptEngineFactory getFactory();

    /**
     * Evaluates {@link Traversal} {@link Bytecode} against a traversal source in the global bindings of the
     * {@code ScriptEngine}.
     *
     * @param bytecode of the traversal to execute
     * @param traversalSource to execute the bytecode against which should be in the available bindings.
     */
    public default Traversal.Admin eval(final Bytecode bytecode, final String traversalSource) throws ScriptException {
        final Bindings bindings = this.createBindings();
        final ScriptContext ctx = this.getContext();

        final Bindings gbindings = ctx.getBindings(ScriptContext.GLOBAL_SCOPE);
        if (gbindings != null) bindings.putAll(gbindings);

        final Bindings ebindings = ctx.getBindings(ScriptContext.ENGINE_SCOPE);
        if (ebindings != null) bindings.putAll(ebindings);

        bindings.putAll(bytecode.getBindings());
        return eval(bytecode, bindings, traversalSource);
    }

    /**
     * Evaluates {@link Traversal} {@link Bytecode} with the specified {@code Bindings}. These {@code Bindings}
     * supplied to this method will be merged with global engine bindings and override them where keys match.
     */
    public Traversal.Admin eval(final Bytecode bytecode, final Bindings bindings, final String traversalSource) throws ScriptException;
}
