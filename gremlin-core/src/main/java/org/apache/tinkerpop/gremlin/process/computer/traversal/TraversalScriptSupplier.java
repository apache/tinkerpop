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
package org.apache.tinkerpop.gremlin.process.computer.traversal;

import org.apache.tinkerpop.gremlin.process.computer.util.ScriptEngineCache;
import org.apache.tinkerpop.gremlin.process.computer.util.ShellGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.Serializable;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalScriptSupplier<S, E> implements Supplier<Traversal.Admin<S, E>>, Serializable {

    private final TraversalSource.Builder traversalContextBuilder;
    private final Class<? extends Graph> graphClass;
    private final String scriptEngineName;
    private final String traversalScript;

    public TraversalScriptSupplier(final Class<? extends Graph> graphClass, final TraversalSource.Builder traversalContextBuilder, final String scriptEngineName, final String traversalScript) {
        this.traversalContextBuilder = traversalContextBuilder;
        this.graphClass = graphClass;
        this.scriptEngineName = scriptEngineName;
        this.traversalScript = traversalScript;
    }

    public Traversal.Admin<S, E> get() {
        try {
            final ScriptEngine engine = ScriptEngineCache.get(this.scriptEngineName);
            final Bindings bindings = engine.createBindings();
            bindings.put("g", this.traversalContextBuilder.create(ShellGraph.of(this.graphClass)));
            return (Traversal.Admin<S, E>) engine.eval(this.traversalScript, bindings);
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
