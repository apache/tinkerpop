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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0, replaced by {@link ScriptTraversal}.
 */
@Deprecated
public final class TraversalScriptHelper {

    private TraversalScriptHelper() {
    }

    @Deprecated
    public static <S, E> Traversal.Admin<S, E> compute(
            final Graph graph,
            final TraversalSource traversalSource,
            final String scriptEngineName,
            final String traversalScript,
            final Object... bindings) {

        try {
            return new TraversalScriptFunction<S, E>(traversalSource, scriptEngineName, traversalScript, bindings).apply(graph);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Deprecated
    public static <S, E> Traversal.Admin<S, E> compute(final String script, final GraphTraversalSource g, final Object... bindings) {
        return new ScriptTraversal<>(g, "gremlin-groovy", script, bindings);
    }
}
