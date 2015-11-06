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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class GroupTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX();

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX_by();

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX();

    public abstract Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byXcountX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX();

    public abstract Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_group_byXname_substring_1X_byXconstantX1XX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXnameX() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_group_byXnameX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXnameX_by() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_group_byXnameX_by();
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXnameX_capXaX() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_groupXaX_byXnameX_capXaX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    private void assertCommonA(Traversal<Vertex, Map<String, Collection<Vertex>>> traversal) {
        final Map<String, Collection<Vertex>> map = traversal.next();
        assertEquals(6, map.size());
        map.forEach((key, values) -> {
            assertEquals(1, values.size());
            assertEquals(convertToVertexId(key), values.iterator().next().id());
        });
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
        final Traversal<Vertex, Map<String, Collection<String>>> traversal = get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        final Map<String, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_group_byXlangX_byXcountX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_hasXlangX_group_byXlangX_byXcountX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Long.valueOf(2), map.get("java"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertTrue(map.containsKey("vadas"));
        assertEquals(Long.valueOf(1), map.get("vadas"));
        assertTrue(map.containsKey("josh"));
        assertEquals(Long.valueOf(1), map.get("josh"));
        assertTrue(map.containsKey("lop"));
        assertEquals(Long.valueOf(4), map.get("lop"));
        assertTrue(map.containsKey("ripple"));
        assertEquals(Long.valueOf(2), map.get("ripple"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXoutE_countX_byXnameX() {
        final Traversal<Vertex, Map<Long, Collection<String>>> traversal = get_g_V_group_byXoutE_countX_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<Long, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertTrue(map.containsKey(0l));
        assertTrue(map.containsKey(1l));
        assertTrue(map.containsKey(2l));
        assertTrue(map.containsKey(3l));
        assertEquals(3, map.get(0l).size());
        assertEquals(1, map.get(1l).size());
        assertEquals(1, map.get(2l).size());
        assertEquals(1, map.get(3l).size());
        assertTrue(map.get(0l).contains("lop"));
        assertTrue(map.get(0l).contains("ripple"));
        assertTrue(map.get(0l).contains("vadas"));
        assertTrue(map.get(1l).contains("peter"));
        assertTrue(map.get(2l).contains("josh"));
        assertTrue(map.get(3l).contains("marko"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(0, map.get("software"));
        assertEquals(3.5d, (double) map.get("person"), 0.01d);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX();
        checkMap(new HashMap<String, Long>() {{
            put("original", 771317l);
            put("", 160968l);
            put("cover", 368579l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX();
        checkMap(new HashMap<String, Long>() {{
            put("original", 771317l);
            put("", 160968l);
            put("cover", 368579l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXname_substring_1X_byXconstantX1XX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_group_byXname_substring_1X_byXconstantX1XX();
        checkMap(new HashMap<String, Long>() {{
            put("m", 1l);
            put("v", 1l);
            put("p", 1l);
            put("l", 1l);
            put("r", 1l);
            put("j", 1l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX();
        checkMap(new HashMap<String, Long>() {{
            put("m", 1l);
            put("v", 1l);
            put("p", 1l);
            put("l", 1l);
            put("r", 1l);
            put("j", 1l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
    }


    public static class Traversals extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            return g.V().<String, Collection<Vertex>>group().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX_by() {
            return g.V().<String, Collection<Vertex>>group().by("name").by();
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            return g.V().<String, Collection<Vertex>>group("a").by("name").cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            return g.V().has("lang").group("a").by("lang").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byXcountX() {
            return g.V().has("lang").<String, Long>group().by("lang").by(count());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX() {
            return g.V().repeat(out().group("a").by("name").by(count())).times(2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            return g.V().<Long, Collection<String>>group().by(outE().count()).by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX() {
            return g.V().<String, Double>group("a").by(T.label).by(outE().values("weight").sum()).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX() {
            return g.V().repeat(both("followedBy")).times(2).<String, Long>group().by("songType").by(count());
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX() {
            return g.V().repeat(both("followedBy")).times(2).<String, Long>group("a").by("songType").by(count()).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_group_byXname_substring_1X_byXconstantX1XX() {
            return g.V().<String, Long>group().<Vertex>by(v -> v.<String>value("name").substring(0, 1)).by(constant(1l));
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX() {
            return g.V().<String, Long>group("a").<Vertex>by(v -> v.<String>value("name").substring(0, 1)).by(constant(1l)).cap("a");
        }
    }
}
