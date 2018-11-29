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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
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

    public abstract Traversal<Vertex, String> get_g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X();

    public abstract Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX();

    public abstract Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Map<Object, Object>>> get_g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX();

    public abstract Traversal<Vertex, Map<Long, Map<String, List<Vertex>>>> get_g_V_group_byXbothE_countX_byXgroup_byXlabelXX();

    public abstract Traversal<Vertex, Map<String, Map<String, Number>>> get_g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX(final Map<String, List<Object>> m);

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXnameX() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_group_byXnameX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXnameX_by() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_group_byXnameX_by();
        printTraversalForm(traversal);
        assertCommonA(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXnameX_capXaX() {
        final Traversal<Vertex, Map<String, Collection<Vertex>>> traversal = get_g_V_groupXaX_byXnameX_capXaX();
        printTraversalForm(traversal);
        assertCommonA(traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
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
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
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
        checkSideEffects(traversal.asAdmin().getSideEffects());
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
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
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
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, map.size());
        assertEquals(3.5d, (double) map.get("person"), 0.01d);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX();
        printTraversalForm(traversal);
        checkMap(new HashMap<String, Long>() {{
            put("original", 771317l);
            put("", 160968l);
            put("cover", 368579l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX();
        printTraversalForm(traversal);
        checkMap(new HashMap<String, Long>() {{
            put("original", 771317l);
            put("", 160968l);
            put("cover", 368579l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXname_substring_1X_byXconstantX1XX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_group_byXname_substring_1X_byXconstantX1XX();
        printTraversalForm(traversal);
        checkMap(new HashMap<String, Long>() {{
            put("m", 1l);
            put("v", 1l);
            put("p", 1l);
            put("l", 1l);
            put("r", 1l);
            put("j", 1l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX();
        printTraversalForm(traversal);
        checkMap(new HashMap<String, Long>() {{
            put("m", 1l);
            put("v", 1l);
            put("p", 1l);
            put("l", 1l);
            put("r", 1l);
            put("j", 1l);
        }}, traversal.next());
        assertFalse(traversal.hasNext());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X() {
        final Traversal<Vertex, String> traversal = get_g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple", "lop"), traversal);
        checkSideEffects(traversal.asAdmin().getSideEffects());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX() {
        final Traversal<Vertex, Map<String, Map<String, Long>>> traversal = get_g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX();
        printTraversalForm(traversal);
        final Map<String, Map<String, Long>> map = traversal.next();
        assertEquals(584, map.size());
        for (final Map.Entry<String, Map<String, Long>> entry : map.entrySet()) {
            assertEquals(entry.getKey().toUpperCase(), entry.getKey());
            final Map<String, Long> countMap = entry.getValue();
            assertEquals(3, countMap.size());
            assertEquals(1l, countMap.get("name").longValue());
            assertEquals(1l, countMap.get("songType").longValue());
            assertEquals(1l, countMap.get("performances").longValue());
        }
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX() {
        final Traversal<Vertex, Map<String, Map<String, Long>>> traversal = get_g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX();
        printTraversalForm(traversal);
        final Map<String, Map<String, Long>> map = traversal.next();
        assertEquals(584, map.size());
        for (final Map.Entry<String, Map<String, Long>> entry : map.entrySet()) {
            assertEquals(entry.getKey().toUpperCase(), entry.getKey());
            final Map<String, Long> countMap = entry.getValue();
            assertEquals(3, countMap.size());
            assertEquals(1l, countMap.get("name").longValue());
            assertEquals(1l, countMap.get("songType").longValue());
            assertEquals(1l, countMap.get("performances").longValue());
        }
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX() {
        //[{a={32=[v[4]], ripple=[v[5], v[5]], vadas=[v[2]], 27=[v[2]], josh=[v[4]], lop=[v[3], v[3], v[3], v[3]]}, b={ripple=2, lop=4}}]
        final Traversal<Vertex, Map<String, Map<Object, Object>>> traversal = get_g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Map<Object, Object>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
        //
        final Map<Object, List<Vertex>> mapA = (Map) map.get("a");
        assertEquals(6, mapA.size());
        assertEquals(1, mapA.get(32).size());
        assertEquals(convertToVertex(graph, "josh"), mapA.get(32).get(0));
        assertEquals(1, mapA.get(27).size());
        assertEquals(convertToVertex(graph, "vadas"), mapA.get(27).get(0));
        assertEquals(2, mapA.get("ripple").size());
        assertEquals(convertToVertex(graph, "ripple"), mapA.get("ripple").get(0));
        assertEquals(convertToVertex(graph, "ripple"), mapA.get("ripple").get(1));
        assertEquals(1, mapA.get("vadas").size());
        assertEquals(convertToVertex(graph, "vadas"), mapA.get("vadas").get(0));
        assertEquals(1, mapA.get("josh").size());
        assertEquals(convertToVertex(graph, "josh"), mapA.get("josh").get(0));
        assertEquals(4, mapA.get("lop").size());
        assertEquals(convertToVertex(graph, "lop"), mapA.get("lop").get(0));
        assertEquals(convertToVertex(graph, "lop"), mapA.get("lop").get(1));
        assertEquals(convertToVertex(graph, "lop"), mapA.get("lop").get(2));
        assertEquals(convertToVertex(graph, "lop"), mapA.get("lop").get(3));
        //
        final Map<String, Long> mapB = (Map) map.get("b");
        assertEquals(2, mapB.size());
        assertEquals(2l, mapB.get("ripple").longValue());
        assertEquals(4l, mapB.get("lop").longValue());
        //
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class, "b", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXbothE_countX_byXgroup_byXlabelXX() {
        final Traversal<Vertex, Map<Long, Map<String, List<Vertex>>>> traversal = get_g_V_group_byXbothE_countX_byXgroup_byXlabelXX();
        printTraversalForm(traversal);
        final Map<Long, Map<String, List<Vertex>>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertTrue(map.containsKey(1l));
        assertTrue(map.containsKey(3l));
        //
        Map<String, List<Vertex>> submap = map.get(1l);
        assertEquals(2, submap.size());
        assertTrue(submap.containsKey("software"));
        assertTrue(submap.containsKey("person"));
        List<Vertex> list = submap.get("software");
        assertEquals(1, list.size());
        assertEquals(convertToVertex(graph, "ripple"), list.get(0));
        list = submap.get("person");
        assertEquals(2, list.size());
        assertTrue(list.contains(convertToVertex(graph, "vadas")));
        assertTrue(list.contains(convertToVertex(graph, "peter")));
        //
        submap = map.get(3l);
        assertEquals(2, submap.size());
        assertTrue(submap.containsKey("software"));
        assertTrue(submap.containsKey("person"));
        list = submap.get("software");
        assertEquals(1, list.size());
        assertEquals(convertToVertex(graph, "lop"), list.get(0));
        list = submap.get("person");
        assertEquals(2, list.size());
        assertTrue(list.contains(convertToVertex(graph, "marko")));
        assertTrue(list.contains(convertToVertex(graph, "josh")));
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX() {
        final Traversal<Vertex, Map<String, Map<String, Number>>> traversal = get_g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX();
        printTraversalForm(traversal);
        final Map<String, Map<String, Number>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3, map.size());
        assertTrue(map.containsKey(""));
        assertTrue(map.containsKey("original"));
        assertTrue(map.containsKey("cover"));
        //
        Map<String, Number> subMap = map.get("");
        assertEquals(1, subMap.size());
        assertEquals(179350, subMap.get("followedBy").intValue());
        //
        subMap = map.get("original");
        assertEquals(1, subMap.size());
        assertEquals(2185613, subMap.get("followedBy").intValue());
        //
        subMap = map.get("cover");
        assertEquals(1, subMap.size());
        assertEquals(777982, subMap.get("followedBy").intValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX();
        printTraversalForm(traversal);
        final Map<String, String> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals("marko", map.get("vadas"));
        assertEquals("marko", map.get("josh"));

        checkSideEffects(traversal.asAdmin().getSideEffects(), "m", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX();
        printTraversalForm(traversal);
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(2.0d, map.get("software").doubleValue(), 0.01d);
        assertEquals(5.0d, map.get("person").doubleValue(), 0.01d);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
        final Map<String, Number> sideEffect = traversal.asAdmin().getSideEffects().get("a");
        assertEquals(2, sideEffect.size());
        assertEquals(4.0d, sideEffect.get("created").doubleValue(), 0.01d);
        assertEquals(3.0d, sideEffect.get("knows").doubleValue(), 0.01d);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX() {
        final Map<String, List<Object>> m = new HashMap<>();
        m.put("marko", new ArrayList<>(Collections.singleton(666)));
        m.put("noone", new ArrayList<>(Collections.singleton("blah")));

        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX(m);
        printTraversalForm(traversal);
        final Map<String, List<Object>> map = traversal.next();
        assertEquals(7, map.size());
        assertEquals(Collections.singleton("blah"), new HashSet<>(map.get("noone")));
        assertEquals(new HashSet<>(Arrays.asList("created", "knows", 666)), new HashSet<>(map.get("marko")));
        assertEquals(Collections.singleton("created"), new HashSet<>(map.get("josh")));
        assertEquals(Collections.singleton("created"), new HashSet<>(map.get("peter")));
        assertEquals(Collections.emptySet(), new HashSet<>(map.get("vadas")));
        assertEquals(Collections.emptySet(), new HashSet<>(map.get("lop")));
        assertEquals(Collections.emptySet(), new HashSet<>(map.get("ripple")));
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", HashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX();
        printTraversalForm(traversal);
        final Map<String, Number> map = traversal.next();
        assertEquals(2, map.size());
        assertTrue(map.containsKey("ripple"));
        assertTrue(map.containsKey("lop"));
        assertEquals(32L, map.get("ripple"));
        assertEquals(96L, map.get("lop"));
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

        @Override
        public Traversal<Vertex, String> get_g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X() {
            return g.V().out().<String, Vertex>group().by(T.label).select("person").unfold().out("created").<String>values("name").limit(2);
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX() {
            return g.V().hasLabel("song").<String, Map<String, Long>>group().by("name").by(__.properties().groupCount().by(T.label));
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Long>>> get_g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX() {
            return g.V().hasLabel("song").group("a").by("name").by(__.properties().groupCount().by(T.label)).out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Object>>> get_g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX() {
            return g.V().repeat(__.union(__.out("knows").group("a").by("age"), __.out("created").group("b").by("name").by(count())).group("a").by("name")).times(2).cap("a", "b");
        }

        @Override
        public Traversal<Vertex, Map<Long, Map<String, List<Vertex>>>> get_g_V_group_byXbothE_countX_byXgroup_byXlabelXX() {
            return g.V().<Long, Map<String, List<Vertex>>>group().by(bothE().count()).by(__.group().by(T.label));
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Number>>> get_g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX() {
            return g.V().out("followedBy").<String, Map<String, Number>>group().by("songType").by(bothE().group().by(T.label).by(values("weight").sum()));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX() {
            return g.V().group("m").by("name").by(__.in("knows").values("name")).cap("m");
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX() {
            return g.V().<String, Number>group().by(T.label).by(bothE().group("a").by(T.label).by(values("weight").sum()).values("weight").sum());
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX(final Map<String, List<Object>> m) {
            return g.withSideEffect("a", m).V().group("a").by("name").by(outE().label().fold()).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX() {
            return g.V().hasLabel("person").as("p").out("created").<String, Number>group().by("name").by(__.select("p").values("age").sum());
        }
    }
}
