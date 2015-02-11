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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.apache.tinkerpop.gremlin.LoadGraphWith;
import com.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.structure.Compare;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class IsTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X();

    public abstract Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X();

    public abstract Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X();

    public abstract Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX();

    public abstract Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXageX_isX32X() {
        Traversal<Vertex, Integer> traversal = get_g_V_valuesXageX_isX32X();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Integer.valueOf(32), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXageX_isXlte_30X() {
        Traversal<Vertex, Integer> traversal = get_g_V_valuesXageX_isXlte_30X();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27, 29), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXageX_isXgte_29X_isXlt_34X() {
        Traversal<Vertex, Integer> traversal = get_g_V_valuesXageX_isXgte_29X_isXlt_34X();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29, 32), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("ripple", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends IsTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            return g.V().<Integer>values("age").is(32);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            return g.V().<Integer>values("age").is(Compare.lte, 30);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            return g.V().<Integer>values("age").is(Compare.gte, 29).is(Compare.lt, 34);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            return g.V().has(in("created").count().is(1l)).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            return g.V().has(in("created").count().is(Compare.gte, 2l)).values("name");
        }
    }

    public static class ComputerTest extends StandardTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isX32X() {
            return super.get_g_V_valuesXageX_isX32X().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXlte_30X() {
            return super.get_g_V_valuesXageX_isXlte_30X().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_valuesXageX_isXgte_29X_isXlt_34X() {
            return super.get_g_V_valuesXageX_isXgte_29X_isXlt_34X().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX() {
            return super.get_g_V_hasXinXcreatedX_count_isX1XX_valuesXnameX().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() {
            return super.get_g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX().submit(g.compute());
        }
    }
}