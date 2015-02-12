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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupCountTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX();


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupCount_byXnameX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_groupCount_byXnameX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertEquals(map.size(), 2);
        assertEquals(Long.valueOf(3l), map.get("lop"));
        assertEquals(Long.valueOf(1l), map.get("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_name_groupCount() {
        Arrays.asList(get_g_V_outXcreatedX_name_groupCount(), get_g_V_outXcreatedX_name_groupCountXaX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Long> map = traversal.next();
            assertEquals(map.size(), 2);
            assertEquals(3l, map.get("lop").longValue());
            assertEquals(1l, map.get("ripple").longValue());
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX_groupCount() {
        final Traversal<Vertex, Map<Object, Long>> traversal = get_g_V_hasXnoX_groupCount();
        printTraversalForm(traversal);
        final Map<Object, Long> map = traversal.next();
        assertEquals(0, map.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
        List<Traversal<Vertex, Map<String, Long>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(4, map.size());
            assertEquals(4l, map.get("lop").longValue());
            assertEquals(2l, map.get("ripple").longValue());
            assertEquals(1l, map.get("josh").longValue());
            assertEquals(1l, map.get("vadas").longValue());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
        List<Traversal<Vertex, Map<String, Long>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(2, map.size());
            assertEquals(2, map.get("marko").longValue());
            assertEquals(2, map.get("java").longValue());
        });
    }

    public static class StandardTest extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            return (Traversal) g.V().out("created").groupCount().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            return (Traversal) g.V().out("created").values("name").groupCount();
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            return (Traversal) g.V().out("created").values("name").groupCount("a");
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount() {
            return (Traversal) g.V().has("no").groupCount();
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            return g.V().repeat(out().groupCount("a").by("name")).times(2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            return g.V().union(
                    repeat(out()).times(2).groupCount("m").by("lang"),
                    repeat(in()).times(2).groupCount("m").by("name")).cap("m");
        }
    }

    public static class ComputerTest extends GroupCountTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            return (Traversal) g.V().out("created").groupCount().by("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            return (Traversal) g.V().out("created").values("name").groupCount().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX() {
            return (Traversal) g.V().out("created").values("name").groupCount("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount() {
            return (Traversal) g.V().has("no").groupCount().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            return g.V().repeat(out().groupCount("a").by("name")).times(2).<Map<String, Long>>cap("a").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            return g.V().union(
                    repeat(out()).times(2).groupCount("m").by("lang"),
                    repeat(in()).times(2).groupCount("m").by("name")).<Map<String, Long>>cap("m").submit(g.compute());
        }
    }
}