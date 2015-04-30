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
package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE

/**
 *
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class GroovyCoalesceTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends CoalesceTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX() {
            g.V().coalesce(out('foo'), out('bar'));
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id) {
            g.V(v1Id).coalesce(out('knows'), out('created')).values('name');
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id) {
            g.V(v1Id).coalesce(out('created'), out('knows')).values('name');
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
            g.V.coalesce(out('likes'), out('knows'), out('created')).groupCount().by('name');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
            g.V.coalesce(outE('knows'), outE('created')).otherV.path.by('name').by(T.label);
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends CoalesceTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX() {
            ComputerTestHelper.compute("g.V().coalesce(out('foo'), out('bar'))", g)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).coalesce(out('knows'), out('created')).values('name')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).coalesce(out('created'), out('knows')).values('name')", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
            ComputerTestHelper.compute("g.V().coalesce(out('likes'), out('knows'), out('created')).groupCount().by('name')", g)
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
        }

        @Override
        Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}
