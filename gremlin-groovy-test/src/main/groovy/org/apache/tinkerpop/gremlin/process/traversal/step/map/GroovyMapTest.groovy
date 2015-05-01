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

import org.apache.tinkerpop.gremlin.LoadGraphWith
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.process.computer.ComputerTestHelper
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroovyMapTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends MapTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            g.V(v1Id).map { v -> v.name };
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            g.V(v1Id).outE.label.map { l -> l.length() };
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            g.V(v1Id).out.map { v -> v.name }.map { n -> n.length() };
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX() {
            g.withPath().V.as('a').out.map { v -> v.path('a').name };
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
            g.withPath().V().as('a').out.out().map { v -> v.path('a').name + v.name };
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends MapTest {

        @Test
        @LoadGraphWith(org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN)
        @Override
        public void g_VX1X_mapXnameX() {
            super.g_VX1X_mapXnameX();
        }

        @Test
        @LoadGraphWith(org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN)
        @Override
        public void g_VX1X_outE_label_mapXlengthX() {
            super.g_VX1X_outE_label_mapXlengthX();
        }

        @Test
        @LoadGraphWith(org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN)
        @Override
        public void g_VX1X_out_mapXnameX_mapXlengthX() {
            super.g_VX1X_out_mapXnameX_mapXlengthX();
        }


        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).map { v -> v.name }", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).outE.label.map { l -> l.length() }", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            ComputerTestHelper.compute("g.V(v1Id).out.map { v -> v.name }.map { n -> n.length() }", g, "v1Id", v1Id);
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_mapXa_nameX() {
            // TODO: Doesn't work for graph computer because sideEffects are not accessible
        }

        @Override
        @Test
        @org.junit.Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_out_mapXa_name_it_nameX() {
            // TODO: Doesn't work for graph computer because sideEffects are not accessible
        }

        @Override
        Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}
