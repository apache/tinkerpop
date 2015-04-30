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

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SideEffectTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_sideEffectXstore_aX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_sideEffectXstore_aX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals(traversal.next(), "marko");
        assertFalse(traversal.hasNext());
        assertEquals(convertToVertexId("marko"), traversal.asAdmin().getSideEffects().<List<Vertex>>get("a").get().get(0).id());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_sideEffectXincr_cX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_out_sideEffectXincr_cX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assert_g_v1_out_sideEffectXincr_cX_valueXnameX(traversal);
        assertEquals(new Integer(3), traversal.asAdmin().getSideEffects().<List<Integer>>get("c").get().get(0));
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

    @UseEngine(TraversalEngine.Type.STANDARD)
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
    }
}
