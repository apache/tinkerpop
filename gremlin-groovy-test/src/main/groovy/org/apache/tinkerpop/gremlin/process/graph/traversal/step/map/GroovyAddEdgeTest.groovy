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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map

import org.apache.tinkerpop.gremlin.process.Traversal
import org.apache.tinkerpop.gremlin.process.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAddEdgeTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a')
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(
                final Object v1Id) {
            g.V(v1Id).as('a').out('created').addOutE('createdBy', 'a', 'weight', 2)
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_addOutEXexistsWith__g_V__time_nowX() {
            g.V.addOutE('existsWith', g.V, 'time', 'now');
        }
    }
}
