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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class CountTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Long> get_g_V_count();

    public abstract Traversal<Vertex, Long> get_g_V_out_count();

    public abstract Traversal<Vertex, Long> get_g_V_both_both_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count();

    public abstract Traversal<Vertex, Long> get_g_V_hasXnoX_count();

    public abstract Traversal<Vertex, Long> get_g_V_fold_countXlocalX();

    public abstract Traversal<Vertex, Long> get_g_V_name_countXlocalX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_count();
        printTraversalForm(traversal);
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_out_count();
        printTraversalForm(traversal);
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_both_both_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_both_both_count();
        printTraversalForm(traversal);
        assertEquals(new Long(1406914), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXoutX_timesX3X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXoutX_timesX3X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(14465066L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXoutX_timesX8X_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXoutX_timesX8X_count();
        printTraversalForm(traversal);
        assertEquals(new Long(2505037961767380L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count();
        printTraversalForm(traversal);
        assertEquals(new Long(24309134024L), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXnoX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXnoX_count();
        printTraversalForm(traversal);
        assertEquals(new Long(0), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_hasXnoX_count();
        printTraversalForm(traversal);
        assertEquals(new Long(0), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_fold_countXlocalX() {
        final Traversal<Vertex, Long> traversal = get_g_V_fold_countXlocalX();
        printTraversalForm(traversal);
        assertEquals(new Long(6), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_countXlocalX() {
        final Traversal<Vertex, Long> traversal = get_g_V_name_countXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(5L, 5L, 3L, 4L, 6L, 5L), traversal);
    }

    public static class Traversals extends CountTest {

        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            return g.V().count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            return g.V().out().count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            return g.V().both().both().count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            return g.V().repeat(out()).times(3).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            return g.V().repeat(out()).times(8).count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count() {
            return g.V().repeat(out()).times(5).as("a").out("writtenBy").as("b").select("a", "b").count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasXnoX_count() {
            return g.V().has("no").count();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_fold_countXlocalX() {
            return g.V().fold().count(Scope.local);
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_name_countXlocalX() {
            return g.V().values("name").count(Scope.local);
        }
    }
}
