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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.jsr223.SingleGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0, replaced by {@link ScriptTraversal}.
 */
public final class TraversalScriptFunction<S, E> implements Function<Graph, Traversal.Admin<S, E>>, Serializable {

    private final TraversalSourceFactory traversalSourceFactory;
    private final String scriptEngineName;
    private final String traversalScript;
    private final Object[] bindings;

    public TraversalScriptFunction(final TraversalSource traversalSource, final String scriptEngineName, final String traversalScript, final Object... bindings) {
        this.traversalSourceFactory = new TraversalSourceFactory<>(traversalSource);
        this.scriptEngineName = scriptEngineName;
        this.traversalScript = traversalScript;
        this.bindings = bindings;
        if (this.bindings.length % 2 != 0)
            throw new IllegalArgumentException("The provided key/value bindings array length must be a multiple of two");
    }

    public Traversal.Admin<S, E> apply(final Graph graph) {
        try {
            final ScriptEngine engine = SingleGremlinScriptEngineManager.get(this.scriptEngineName);
            final Bindings engineBindings = engine.createBindings();
            engineBindings.put("g", this.traversalSourceFactory.createTraversalSource(graph));
            for (int i = 0; i < this.bindings.length; i = i + 2) {
                engineBindings.put((String) this.bindings[i], this.bindings[i + 1]);
            }
            final Traversal.Admin<S, E> traversal = (Traversal.Admin<S, E>) engine.eval(this.traversalScript, engineBindings);
            if (!traversal.isLocked()) traversal.applyStrategies();
            return traversal;
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public String toString() {
        return this.traversalScript;
    }
}
