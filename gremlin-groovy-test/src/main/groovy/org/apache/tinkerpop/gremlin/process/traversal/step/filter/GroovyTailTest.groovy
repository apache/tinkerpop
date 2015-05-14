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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.limit;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.global;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public abstract class GroovyTailTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends TailTest {

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailXglobal_2X() {
            g.V.values('name').order.tail(global, 2)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX2X() {
            g.V.values('name').order.tail(2)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tail() {
            g.V.values('name').order.tail
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX7X() {
            g.V.values('name').order.tail(7)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_tailX7X() {
            g.V.repeat(both()).times(3).tail(7);
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X() {
            g.V.as('a').out.as('a').out.as('a').select('a').by(unfold().values('name').fold).tail(local, 2)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X() {
            g.V.as('a').out.as('a').out.as('a').select('a').by(unfold().values('name').fold).tail(local, 1)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX() {
            g.V.as('a').out.as('a').out.as('a').select('a').by(unfold().values('name').fold).tail(local)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
            g.V.as('a').out.as('a').out.as('a').select('a').by(limit(local, 0)).tail(local, 1)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_2X() {
            g.V.as('a').out.as('b').out.as('c').select.by(T.id).tail(local, 2)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_1X() {
            g.V.as('a').out.as('b').out.as('c').select.by(T.id).tail(local, 1)
        }
    }

    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class ComputerTraversals extends TailTest {

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_valuesXnameX_order_tailXglobal_2X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_valuesXnameX_order_tailX2X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_valuesXnameX_order_tail() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_valuesXnameX_order_tailX7X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_repeatXbothX_timesX3X_tailX7X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_2X() {
        }

        @Override
        @Test
        @Ignore("Traversal not supported by ComputerTraversalEngine.computer")
        public void g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_1X() {
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailXglobal_2X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX2X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tail() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_order_tailX7X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_tailX7X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_2X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, String> get_g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXlimitXlocal_0XX_tailXlocal_1X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_2X() {
            // override with nothing until the test itself is supported
            return null
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_asXaX_out_asXbX_out_asXcX_select_byXnameX_tailXlocal_1X() {
            // override with nothing until the test itself is supported
            return null
        }
    }
}
