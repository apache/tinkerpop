/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ArrayListSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that cover lambda steps that are used in embedded cases.
 */
@RunWith(GremlinProcessRunner.class)
public abstract class LambdaStepTest extends AbstractGremlinProcessTest {

    /// branch

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX();

    /// choose

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name();

    /// filter

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXfalseX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXtrueX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXfalseX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXtrueX();

    /// flatMap

    public abstract Traversal<Vertex, String> get_g_V_valuesXnameX_flatMapXsplitXaX();

    ///  group

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_group_byXname_substring_1X_byXconstantX1XX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX();

    ///  has

    public abstract Traversal<Vertex, String> get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name();

    /// inject

    public abstract Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id);

    /// map

    public abstract Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id);

    public abstract Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX();

    public abstract Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX();

    /// order

    public abstract Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name();

    public abstract Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX(final Object v1Id);

    /// repeat

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX();

    // sideEffect

    public abstract Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, Long>> get_g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX();

    public abstract Traversal<Vertex, Integer> get_g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX();

    public abstract Traversal<Vertex, Integer> get_g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX();

    public abstract Traversal<Vertex, String> get_g_withSideEffectXk_nameX_V_order_byXvalueMap_selectXkX_unfoldX_name();

    ///  unfold

    public abstract Traversal<Vertex, String> get_g_V_valueMap_unfold_mapXkeyX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "vadas", "josh", "josh", "marko", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(5, map.size());
        assertEquals(3L, map.get("ripple").longValue());
        assertEquals(3L, map.get("vadas").longValue());
        assertEquals(4L, map.get("josh").longValue());
        assertEquals(10L, map.get("lop").longValue());
        assertEquals(4L, map.get("marko").longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXfalseX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXfalseX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXtrueX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXtrueX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXlang_eq_javaX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXlang_eq_javaX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("ripple") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_filterXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_filterXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_filterXage_gt_30X() {
        final  Traversal<Vertex, Vertex> traversal = get_g_VX1X_filterXage_gt_30X(convertToVertexId("josh"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_filterXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_filterXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("marko") ||
                    vertex.value("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXfalseX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXfalseX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXtrueX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXtrueX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, edges.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("ripple"), traversal);
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_mapXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_mapXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_label_mapXlengthX() {
        final Traversal<Vertex, Integer> traversal = get_g_VX1X_outE_label_mapXlengthX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Integer> lengths = traversal.toList();
        assertTrue(lengths.contains("created".length()));
        assertTrue(lengths.contains("knows".length()));
        assertEquals(lengths.size(), 3);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_mapXnameX_mapXlengthX() {
        final Traversal<Vertex, Integer> traversal = get_g_VX1X_out_mapXnameX_mapXlengthX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final List<Integer> lengths = traversal.toList();
        assertTrue(lengths.contains("josh".length()));
        assertTrue(lengths.contains("vadas".length()));
        assertTrue(lengths.contains("lop".length()));
        assertEquals(lengths.size(), 3);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_withPath_V_asXaX_out_mapXa_nameX() {
        int marko = 0;
        int peter = 0;
        int josh = 0;
        int other = 0;

        final Traversal<Vertex, String> traversal = get_g_withPath_V_asXaX_out_mapXa_nameX();
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            final String name = traversal.next();
            if (name.equals("marko")) marko++;
            else if (name.equals("peter")) peter++;
            else if (name.equals("josh")) josh++;
            else other++;
        }
        assertEquals(marko, 3);
        assertEquals(josh, 2);
        assertEquals(peter, 1);
        assertEquals(other, 0);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
        final Traversal<Vertex, String> traversal = get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String doubleName = traversal.next();
            assertTrue("markoripple".equals(doubleName) || "markolop".equals(doubleName));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList("peter", "josh", "marko", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX() {
        final Traversal<Vertex, Map<Integer, Integer>> traversal = get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Map<Integer, Integer> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        final Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
        Map.Entry<Integer, Integer> entry = iterator.next();
        assertEquals(3, entry.getKey().intValue());
        assertEquals(87, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(2, entry.getKey().intValue());
        assertEquals(58, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(1, entry.getKey().intValue());
        assertEquals(29, entry.getValue().intValue());
        entry = iterator.next();
        assertEquals(4, entry.getKey().intValue());
        assertEquals(29, entry.getValue().intValue());
        assertFalse(iterator.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap_unfold_mapXkeyX() {
        final Traversal<Vertex, String> traversal = get_g_V_valueMap_unfold_mapXkeyX();
        printTraversalForm(traversal);
        int counter = 0;
        int ageCounter = 0;
        int nameCounter = 0;
        int langCounter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String key = traversal.next();
            if (key.equals("name"))
                nameCounter++;
            else if (key.equals("age"))
                ageCounter++;
            else if (key.equals("lang"))
                langCounter++;
            else
                fail("The provided key is not known: " + key);
        }
        assertEquals(12, counter);
        assertEquals(4, ageCounter);
        assertEquals(2, langCounter);
        assertEquals(6, nameCounter);
        assertEquals(counter, ageCounter + langCounter + nameCounter);
        assertFalse(traversal.hasNext());
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
    public void g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            if (path.get("a").equals("daniel")) {
                assertEquals(2, path.size());
                assertEquals(6, (int) path.get(1));
            } else {
                assertEquals(4, path.size());
                assertEquals(path.<String>get("a").length(), (int) path.get(3));
            }
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_sideEffectXstore_aX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_sideEffectXstore_aX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
        assertEquals(convertToVertexId("marko"), traversal.asAdmin().getSideEffects().<List<Vertex>>get("a").get(0).id());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", ArrayList.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_out_sideEffectXincr_cX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_sideEffectXincr_cX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
        assertEquals(new Integer(3), traversal.asAdmin().getSideEffects().<List<Integer>>get("c").get(0));
        checkSideEffects(traversal.asAdmin().getSideEffects(), "c", ArrayList.class);
    }

    private void assert_g_v1_out_sideEffectXincr_cX_valueXnameX(final Iterator<String> traversal) {
        final List<String> names = new ArrayList<>();
        while (traversal.hasNext()) {
            names.add(traversal.next());
        }
        assertEquals(3, names.size());
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_sideEffectXX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_sideEffectXX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX();
        printTraversalForm(traversal);
        Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(4l, map.get("software").longValue());
        assertEquals(2l, map.get("person").longValue());
        final TraversalSideEffects sideEffects = traversal.asAdmin().getSideEffects();
        map = sideEffects.get("a");
        assertEquals(2, map.size());
        assertEquals(4l, map.get("software").longValue());
        assertEquals(2l, map.get("person").longValue());
        ///
        assertEquals(1, sideEffects.keys().size());
        assertTrue(sideEffects.keys().contains("a"));
        assertTrue(sideEffects.exists("a"));
        assertTrue(sideEffects.get("a") instanceof LinkedHashMap);
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", LinkedHashMap.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX();
        printTraversalForm(traversal);
        Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(2l, map.get("software").longValue());
        assertEquals(4l, map.get("person").longValue());
        final TraversalSideEffects sideEffects = traversal.asAdmin().getSideEffects();
        map = sideEffects.get("a");
        assertEquals(2, map.size());
        assertEquals(2l, map.get("software").longValue());
        assertEquals(4l, map.get("person").longValue());
        ///
        assertEquals(3, sideEffects.keys().size());
        assertTrue(sideEffects.keys().contains("a"));
        assertTrue(sideEffects.exists("a"));
        assertTrue(sideEffects.get("a") instanceof LinkedHashMap);
        //
        assertTrue(sideEffects.keys().contains("b"));
        assertTrue(sideEffects.exists("b"));
        assertTrue(sideEffects.get("b") instanceof ArrayList);
        assertEquals(18, sideEffects.<List<Integer>>get("b").size());
        assertEquals(6l, sideEffects.<List<Integer>>get("b").stream().filter(t -> t == 1).count());
        assertEquals(6l, sideEffects.<List<Integer>>get("b").stream().filter(t -> t == 2).count());
        assertEquals(6l, sideEffects.<List<Integer>>get("b").stream().filter(t -> t == 3).count());
        //
        assertTrue(sideEffects.keys().contains("c"));
        assertTrue(sideEffects.exists("c"));
        assertTrue(sideEffects.get("c") instanceof ArrayList);
        assertEquals(0, sideEffects.<List>get("c").size());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", LinkedHashMap.class, "b", ArrayList.class, "c", ArrayList.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX() {
        final Traversal<Vertex, Integer> traversal = get_g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX();
        assertEquals(6, traversal.next().intValue());
        assertFalse(traversal.hasNext());
        assertEquals(6, traversal.asAdmin().getSideEffects().<Integer>get("a").intValue());
        assertEquals(1, traversal.asAdmin().getSideEffects().keys().size());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", Integer.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX() {
        final Traversal<Vertex, Integer> traversal = get_g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX();
        assertEquals(1, traversal.next().intValue());
        assertFalse(traversal.hasNext());
        assertEquals(1, traversal.asAdmin().getSideEffects().<Integer>get("a").intValue());
        assertEquals(1, traversal.asAdmin().getSideEffects().keys().size());
        checkSideEffects(traversal.asAdmin().getSideEffects(), "a", Integer.class);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXk_nameX_V_order_byXvalueMap_selectXkX_unfoldX_name() {
        final Traversal<Vertex, String> traversal = get_g_withSideEffectXk_nameX_V_order_byXvalueMap_selectXkX_unfoldX_name();
        checkOrderedResults(Arrays.asList(
                "josh", "lop", "marko", "peter", "ripple", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valuesXnameX_flatMapXsplitXaX() {
        final Traversal<Vertex, String> traversal = get_g_V_valuesXnameX_flatMapXsplitXaX();
        checkResults(Arrays.asList(
                "josh", "lop", "rko", "peter", "ripple", "v", "d", "s", "m"), traversal);
    }

    public static class Traversals extends LambdaStepTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .option("a", values("age"))
                    .option("b", values("lang"))
                    .option("b", values("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name() {
            return g.V().choose(v -> v.label().equals("person"), out("knows"), in("created")).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXbothX_untilXname_eq_marko_or_loops_gt_1X_groupCount_byXnameX() {
            return g.V().repeat(both()).until(t -> t.get().value("name").equals("lop") || t.loops() > 1).<String>groupCount().by("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>property("lang").orElse("none").equals("java"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_filterXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_filterXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).out().filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().value("name");
                return name.startsWith("m") || name.startsWith("p");
            });
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
            return g.E().filter(e -> false);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
            return g.E().filter(e -> true);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name() {
            return g.V().out("created").has("name", __.<String, Integer>map(s -> s.get().length()).is(P.gt(3))).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_mapXnameX(final Object v1Id) {
            return g.V(v1Id).<String>map(v -> v.get().value("name"));
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_outE_label_mapXlengthX(final Object v1Id) {
            return g.V(v1Id).outE().label().map(l -> l.get().length());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_VX1X_out_mapXnameX_mapXlengthX(final Object v1Id) {
            return g.V(v1Id).out().map(v -> v.get().value("name")).map(n -> n.get().toString().length());
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_mapXa_nameX() {
            return g.withPath().V().as("a").out().<String>map(v -> v.<Vertex>path("a").value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_withPath_V_asXaX_out_out_mapXa_name_it_nameX() {
            return g.withPath().V().as("a").out().out().map(v -> v.<Vertex>path("a").<String>value("name") + v.get().<String>value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasLabelXpersonX_order_byXvalueXageX_descX_name() {
            return g.V().hasLabel("person").order().<Vertex>by(v -> v.value("age"), desc).values("name");
        }

        @Override
        public Traversal<Vertex, Map<Integer, Integer>> get_g_VX1X_hasXlabel_personX_mapXmapXint_ageXX_orderXlocalX_byXvalues_descX_byXkeys_ascX(final Object v1Id) {
            return g.V(v1Id).hasLabel("person").map(v -> {
                final Map<Integer, Integer> map = new HashMap<>();
                map.put(1, (int) v.get().value("age"));
                map.put(2, (int) v.get().value("age") * 2);
                map.put(3, (int) v.get().value("age") * 3);
                map.put(4, (int) v.get().value("age"));
                return map;
            }).order(Scope.local).by(Column.values, desc).by(keys, Order.asc);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valueMap_unfold_mapXkeyX() {
            return g.V().valueMap().<Map.Entry<String, List>>unfold().map(m -> m.get().getKey());
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
        public Traversal<Vertex, Path> get_g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            return g.V(v1Id).out().<String>values("name").inject("daniel").as("a").map(t -> t.get().length()).path();
        }
        @Override
        public Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id) {
            return g.withSideEffect("a", ArrayList::new).V(v1Id).sideEffect(traverser -> {
                traverser.<List>sideEffects("a").clear();
                traverser.<List<Vertex>>sideEffects("a").add(traverser.get());
            }).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id) {
            return g.withSideEffect("c", () -> {
                final List<Integer> list = new ArrayList<>();
                list.add(0);
                return list;
            }).V(v1Id).out().sideEffect(traverser -> {
                Integer temp = traverser.<List<Integer>>sideEffects("c").get(0);
                traverser.<List<Integer>>sideEffects("c").clear();
                traverser.<List<Integer>>sideEffects("c").add(temp + 1);
            }).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id) {
            return g.V(v1Id).out().sideEffect(traverser -> {
            }).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX() {
            return g.withSideEffect("a", new LinkedHashMapSupplier()).V().out().groupCount("a").by(T.label).out().out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX() {
            return g.withSideEffect("a", new LinkedHashMapSupplier())
                    .withSideEffect("b", ArrayListSupplier.instance(), Operator.addAll)
                    .withSideEffect("c", ArrayListSupplier.instance(), Operator.addAll)
                    .V().group("a").by(T.label).by(__.count())
                    .sideEffect(t -> t.sideEffects("b", new LinkedList<>(Arrays.asList(1, 2, 3))))
                    .out().out().out()
                    .sideEffect(t -> t.sideEffects("c", new LinkedList<>(Arrays.asList("bob", "daniel"))))
                    .cap("a");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX() {
            return g.withSideEffect("a", 0, Operator.sum).V().out().sideEffect(t -> t.sideEffects("a", (int) t.bulk())).cap("a");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX() {
            return g.withSideEffect("a", 0).V().out().sideEffect(t -> t.sideEffects("a", 1)).cap("a");
        }

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXk_nameX_V_order_byXvalueMap_selectXkX_unfoldX_name() {
            return g.withSideEffect("key","name").V().order().by(valueMap().select(select("key")).unfold()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_valuesXnameX_flatMapXsplitXaX() {
            return g.V().values("name").flatMap(s -> Stream.of(((String) s.get()).split("a")).iterator());
        }
    }

    public static class LinkedHashMapSupplier implements Supplier<LinkedHashMap> {

        @Override
        public LinkedHashMap get() {
            return new LinkedHashMap();
        }

    }
}
