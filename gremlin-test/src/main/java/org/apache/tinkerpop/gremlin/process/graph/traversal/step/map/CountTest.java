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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CountTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Long> get_g_V_count();

    public abstract Traversal<Vertex, Long> get_g_V_out_count();

    public abstract Traversal<Vertex, Long> get_g_V_both_both_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count();

    public abstract Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count();

    public abstract Traversal<Vertex, Long> get_g_V_filterXfalseX_count();

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
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX_count() {
        final Traversal<Vertex, Long> traversal = get_g_V_filterXfalseX_count();
        printTraversalForm(traversal);
        assertEquals(new Long(0), traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends CountTest {

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
        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            return g.V().filter(v -> false).count();
        }
    }

    public static class ComputerTest extends CountTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_count() {
            return g.V().count().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_out_count() {
            return g.V().out().count().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_both_both_count() {
            return g.V().both().both().count().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX3X_count() {
            return g.V().repeat(out()).times(3).count().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_repeatXoutX_timesX8X_count() {
            return g.V().repeat(out()).times(8).count().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_filterXfalseX_count() {
            return g.V().filter(v -> false).count().submit(g.compute());
        }
    }
}
