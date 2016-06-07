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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ArrayListSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SideEffectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, Long>> get_g_withSideEffectXa__linkedhashmapX_V_out_groupCountXaX_byXlabelX_out_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_withSideEffectsXa__linkedhashmapX_withSideEffectXb__arraylist__addAllX_withSideEffectXc__arrayList__addAllX_V_groupXaX_byXlabelX_byXcountX_sideEffectXb__1_2_3X_out_out_out_sideEffectXc__bob_danielX_capXaX();

    public abstract Traversal<Vertex, Integer> get_g_withSideEffectXa_0_sumX_V_out_sideEffectXsideEffectsXa_bulkXX_capXaX();

    public abstract Traversal<Vertex, Integer> get_g_withSideEffectXa_0X_V_out_sideEffectXsideEffectsXa_1XX_capXaX();

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

    public static class Traversals extends SideEffectTest {

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
    }

    private static class LinkedHashMapSupplier implements Supplier<LinkedHashMap> {

        @Override
        public LinkedHashMap get() {
            return new LinkedHashMap();
        }

    }
}
