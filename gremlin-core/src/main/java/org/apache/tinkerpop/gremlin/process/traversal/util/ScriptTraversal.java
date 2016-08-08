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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.List;

/**
 * ScriptTraversal encapsulates a {@link ScriptEngine} and a script which is compiled into a {@link Traversal} at {@link Admin#applyStrategies()}.
 * This is useful for serializing traversals as the compilation can happen on the remote end where the traversal will ultimately be processed.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ScriptTraversal<S, E> extends DefaultTraversal<S, E> {

    private final String alias;
    private final TraversalSourceFactory factory;
    private final String script;
    private final String scriptEngine;
    private final Object[] bindings;

    public ScriptTraversal(final TraversalSource traversalSource, final String scriptEngine, final String script, final Object... bindings) {
        super();
        this.alias = "g";
        this.graph = traversalSource.getGraph();
        this.factory = new TraversalSourceFactory<>(traversalSource);
        this.scriptEngine = scriptEngine;
        this.script = script;
        this.bindings = bindings;
        if (this.bindings.length % 2 != 0)
            throw new IllegalArgumentException("The provided key/value bindings array length must be a multiple of two");
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        try {
            assert 0 == this.getSteps().size();
            final ScriptEngine engine = SingleGremlinScriptEngineManager.get(this.scriptEngine);
            final Bindings engineBindings = engine.createBindings();
            final List<TraversalStrategy<?>> strategyList = this.getStrategies().toList();
            engineBindings.put(this.alias, this.factory.createTraversalSource(this.graph).withStrategies(strategyList.toArray(new TraversalStrategy[strategyList.size()])));
            engineBindings.put("graph", this.graph); // TODO: we don't need this as the traversalSource.getGraph() exists, but its now here and people might be using it (remove in 3.3.0)
            for (int i = 0; i < this.bindings.length; i = i + 2) {
                engineBindings.put((String) this.bindings[i], this.bindings[i + 1]);
            }
            final Traversal.Admin<S, E> traversal = (Traversal.Admin<S, E>) engine.eval(this.script, engineBindings);
            traversal.getSideEffects().mergeInto(this.sideEffects);
            traversal.getSteps().forEach(this::addStep);
            this.strategies = traversal.getStrategies();
            super.applyStrategies();
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
