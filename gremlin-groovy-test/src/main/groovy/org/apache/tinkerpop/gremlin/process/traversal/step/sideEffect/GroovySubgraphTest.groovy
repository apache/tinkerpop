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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySubgraphTest {

    public static class Traversals extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(
                final Object v1Id, final Graph subgraph) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('sg') { subgraph }.V(v1Id).outE('knows').subgraph('sg').name.cap('sg')", "v1Id", v1Id, "subgraph", subgraph)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(
                final Graph subgraph) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('sg') { subgraph }.V.repeat(__.bothE('created').subgraph('sg').outV).times(5).name.dedup", "subgraph", subgraph)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(final Graph subgraph) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('sg') { subgraph }.V.has('name','daniel').outE.subgraph('sg').inV", "subgraph", subgraph)
        }
    }
}
