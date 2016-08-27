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
package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.jsr223.SingleGremlinScriptEngineManager;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScriptEngineLambda implements Function, Supplier, Consumer, Predicate, BiConsumer, TriConsumer {

    private static final String A = "a";
    private static final String B = "b";
    private static final String C = "c";

    protected final ScriptEngine engine;
    protected final String script;

    public ScriptEngineLambda(final String engineName, final String script) {
        this.engine = SingleGremlinScriptEngineManager.get(engineName);
        this.script = script;
    }

    public Object apply(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            return this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public Object get() {
        try {
            return this.engine.eval(this.script);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a, final Object b) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            bindings.put(B, b);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public void accept(final Object a, final Object b, final Object c) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            bindings.put(B, b);
            bindings.put(C, c);
            this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public boolean test(final Object a) {
        try {
            final Bindings bindings = new SimpleBindings();
            bindings.put(A, a);
            return (boolean) this.engine.eval(this.script, bindings);
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

}
