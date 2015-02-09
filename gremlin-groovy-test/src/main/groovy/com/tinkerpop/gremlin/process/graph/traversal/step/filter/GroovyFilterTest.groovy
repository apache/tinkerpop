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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.ComputerTestHelper
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.FilterTest
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyFilterTest {

    public static class StandardTest extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            g.V.filter { false }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            g.V.filter { true }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            g.V.filter { it.property('lang').orElse("none") == 'java' }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id) {
            g.V(v1Id).filter { it.age > 30 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id) {
            g.V(v1Id).out.filter { it.property('age').orElse(0) > 30 }
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            g.V.filter { it.name.startsWith('m') || it.name.startsWith('p') }
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            g.E.filter { false }
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            g.E.filter { true }
        }
    }

    public static class ComputerTest extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            ComputerTestHelper.compute("g.V.filter { false }", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            ComputerTestHelper.compute("g.V.filter { true }", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            ComputerTestHelper.compute("g.V.filter { it.property('lang').orElse('none') == 'java' }", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).filter { it.age > 30 }", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).out.filter { it.property('age').orElse(0) > 30 }", g);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            ComputerTestHelper.compute("g.V.filter { it.name.startsWith('m') || it.name.startsWith('p') }", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            ComputerTestHelper.compute("g.E.filter { false }", g);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            ComputerTestHelper.compute("g.E.filter { true }", g);
        }
    }
}
