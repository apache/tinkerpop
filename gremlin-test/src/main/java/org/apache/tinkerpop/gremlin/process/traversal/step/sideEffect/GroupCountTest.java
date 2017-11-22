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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class GroupCountTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount();

    public abstract Traversal<Vertex, Map<Vertex, Long>> get_g_V_outXcreatedX_groupCountXxX_capXxX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX_capXaX();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount();

    public abstract Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCountXaX_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX();

    public abstract Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX();

    public abstract Traversal<Vertex, Long> get_g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum();

    public abstract Traversal<Vertex, Map<Vertex, Long>> get_g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX();

    public abstract Traversal<Vertex, String> get_g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupCount_byXnameX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_groupCount_byXnameX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    private static void assertCommonA(final Traversal<Vertex, Map<String, Long>> traversal) {
        final Map<String, Long> map = traversal.next();
        assertEquals(map.size(), 2);
        assertEquals(Long.valueOf(3l), map.get("lop"));
        assertEquals(Long.valueOf(1l), map.get("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_groupCountXxX_capXxX() {
        final Traversal<Vertex, Map<Vertex, Long>> traversal = get_g_V_outXcreatedX_groupCountXxX_capXxX();
        final Object lopId = convertToVertexId("lop");
        final Object rippleId = convertToVertexId("ripple");
        printTraversalForm(traversal);
        final Map<Vertex, Long> map = traversal.next();
        assertEquals(map.size(), 2);
        boolean hasLop = false, hasRipple = false, hasSomethingElse = false;
        for (final Map.Entry<Vertex, Long> entry : map.entrySet()) {
            final Object id = entry.getKey().id();
            if (lopId.equals(id)) {
                hasLop = true;
                assertEquals(Long.valueOf(3l), entry.getValue());
            } else if (rippleId.equals(id)) {
                hasRipple = true;
                assertEquals(Long.valueOf(1l), entry.getValue());
            } else {
                hasSomethingElse = true;
            }
        }
        assertTrue(hasLop);
        assertTrue(hasRipple);
        assertFalse(hasSomethingElse);
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "x", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_name_groupCount() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_name_groupCount();
        printTraversalForm(traversal);
        assertCommonB(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_name_groupCountXaX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_outXcreatedX_name_groupCountXaX_capXaX();
        printTraversalForm(traversal);
        assertCommonB(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    private static void assertCommonB(final Traversal<Vertex, Map<String, Long>> traversal) {
        final Map<String, Long> map = traversal.next();
        assertEquals(map.size(), 2);
        assertEquals(3l, map.get("lop").longValue());
        assertEquals(1l, map.get("ripple").longValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXnoX_groupCount() {
        final Traversal<Vertex, Map<Object, Long>> traversal = get_g_V_hasXnoX_groupCount();
        printTraversalForm(traversal);
        assertCommonC(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXnoX_groupCountXaX_capXaX() {
        final Traversal<Vertex, Map<Object, Long>> traversal = get_g_V_hasXnoX_groupCountXaX_capXaX();
        printTraversalForm(traversal);
        assertCommonC(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    private static void assertCommonC(final Traversal<Vertex, Map<Object, Long>> traversal) {
        final Map<Object, Long> map = traversal.next();
        assertEquals(0, map.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertEquals(4l, map.get("lop").longValue());
        assertEquals(2l, map.get("ripple").longValue());
        assertEquals(1l, map.get("josh").longValue());
        assertEquals(1l, map.get("vadas").longValue());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(2, map.get("marko").longValue());
        assertEquals(2, map.get("java").longValue());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "m", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupCount_byXbothE_countX() {
        final Traversal<Vertex, Map<Long, Long>> traversal = get_g_V_groupCount_byXbothE_countX();
        printTraversalForm(traversal);
        checkMap(new HashMap<Long, Long>() {{
            put(1l, 3l);
            put(3l, 3l);
        }}, traversal.next());
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum() {
        final Traversal<Vertex, Long> traversal = get_g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum();
        printTraversalForm(traversal);
        assertEquals(12l, traversal.next().longValue());
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX() {
        final Traversal<Vertex, Map<Vertex, Long>> traversal = get_g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX();
        printTraversalForm(traversal);
        //  [{v[1]=6, v[2]=2, v[3]=6, v[4]=6, v[5]=2, v[6]=2}]
        final Map<Vertex, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(6, map.size());
        assertEquals(6l, map.get(convertToVertex(graph, "marko")).longValue());
        assertEquals(2l, map.get(convertToVertex(graph, "vadas")).longValue());
        assertEquals(6l, map.get(convertToVertex(graph, "lop")).longValue());
        assertEquals(6l, map.get(convertToVertex(graph, "josh")).longValue());
        assertEquals(2l, map.get(convertToVertex(graph, "ripple")).longValue());
        assertEquals(6l, map.get(convertToVertex(graph, "marko")).longValue());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "lop", "lop", "peter", "marko", "marko", "marko", "ripple", "vadas", "josh", "josh", "josh"), traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    public static class Traversals extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            return g.V().out("created").<String>groupCount().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX() {
            return g.V().out("created").<String>groupCount("a").by("name").cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            return g.V().out("created").values("name").groupCount();
        }

        @Override
        public Traversal<Vertex, Map<Vertex, Long>> get_g_V_outXcreatedX_groupCountXxX_capXxX() {
            return g.V().out("created").groupCount("x").cap("x");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX_capXaX() {
            return g.V().out("created").values("name").groupCount("a").cap("a");
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount() {
            return g.V().has("no").groupCount();
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCountXaX_capXaX() {
            return g.V().has("no").groupCount("a").cap("a");
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

        @Override
        public Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX() {
            return g.V().<Long>groupCount().by(bothE().count());
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum() {
            return g.V().union(out("knows"), out("created").in("created")).groupCount().select(Column.values).unfold().sum();
        }

        @Override
        public Traversal<Vertex, Map<Vertex, Long>> get_g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX() {
            return g.V().both().groupCount("a").out().cap("a").select(Column.keys).unfold().both().groupCount("a").cap("a");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name() {
            return g.V().both().groupCount("a").by(T.label).as("b").barrier().where(__.select("a").select("software").is(gt(2))).select("b").values("name");
        }
    }
}