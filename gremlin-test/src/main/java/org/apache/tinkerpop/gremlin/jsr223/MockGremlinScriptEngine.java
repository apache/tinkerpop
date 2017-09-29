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

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.Reader;

/**
 * This is a "do nothing" implementation of the {@link GremlinScriptEngine} which can be used to help test plugin
 * implementations which don't have reference to a {@link GremlinScriptEngine} as a dependency.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MockGremlinScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
    @Override
    public GremlinScriptEngineFactory getFactory() {
        return new MockGremlinScriptEngineFactory();
    }

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        return null;
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return null;
    }

    @Override
    public Traversal.Admin eval(Bytecode bytecode, Bindings bindings, String traversalSource) throws ScriptException {
        return null;
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }
}
