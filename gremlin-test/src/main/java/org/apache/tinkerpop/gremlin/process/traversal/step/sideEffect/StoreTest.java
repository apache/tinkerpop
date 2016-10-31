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
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class StoreTest extends AbstractGremlinProcessTest {

    @SafeVarargs
    private static <E> BulkSet<E> makeBulkSet(final E... elements) {
        final BulkSet<E> result = new BulkSet<>();
        Collections.addAll(result, elements);
        return result;
    }

    public abstract Traversal<Vertex, Collection> get_g_V_storeXaX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Collection> get_g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX(final Object v1Id);

    public abstract Traversal<Vertex, Set<String>> get_g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX();

    public abstract Traversal<Vertex, Collection> get_g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX_capXaX();

    public abstract Traversal<Vertex, BulkSet<String>> get_g_V_order_byXidX_storeXaX_byXnameX_out_selectXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_storeXa_nameX_out_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_V_storeXaX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        final Collection names = traversal.next();
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("vadas"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        final Collection names = traversal.next();
        assertEquals(4, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("lop"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX() {
        final Traversal<Vertex, Set<String>> traversal = get_g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX();
        printTraversalForm(traversal);
        final Set<String> names = traversal.next();
        assertFalse(traversal.hasNext());
        assertFalse(names instanceof BulkSet);
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("ripple"));
        assertTrue(names.contains("peter"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX() {
        final Traversal<Vertex, Collection> traversal = get_g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX_capXaX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Collection store = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(8, store.size());
        assertTrue(store.contains(0L));
        assertTrue(store.contains(1L));
        assertTrue(store.contains(2L));
        assertTrue(store.contains(1.0d));
        assertFalse(store.isEmpty());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_order_byXidX_storeXaX_byXnameX_out_selectXaX() {
        final Traversal<Vertex, BulkSet<String>> traversal = get_g_V_order_byXidX_storeXaX_byXnameX_out_selectXaX();
        printTraversalForm(traversal);
        final List<BulkSet<String>> expected = new ArrayList<>(6);
        final boolean onGraphComputer = traversal.asAdmin().getStrategies().getStrategy(VertexProgramStrategy.class).isPresent();
        if (onGraphComputer || graph instanceof RemoteGraph) {
            while (expected.size() < 6) {
                expected.add(makeBulkSet("marko", "vadas", "lop", "josh", "ripple", "peter"));
            }
        } else {
            expected.addAll(Arrays.asList(
                    makeBulkSet("marko"),
                    makeBulkSet("marko"),
                    makeBulkSet("marko"),
                    makeBulkSet("marko", "vadas", "lop", "josh"),
                    makeBulkSet("marko", "vadas", "lop", "josh"),
                    makeBulkSet("marko", "vadas", "lop", "josh", "ripple", "peter")));
        }
        while (traversal.hasNext()) {
            final BulkSet<String> a = traversal.next();
            boolean match = false;
            for (int i = 0; i < expected.size(); i++) {
                if (match = expected.get(i).equals(a)) {
                    expected.remove(i);
                    break;
                }
            }
            assertTrue("" + a + " should match any item of " + expected + ".", match);
        }
        assertTrue(expected.isEmpty());
    }

    public static class Traversals extends StoreTest {
        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXnameX_out_capXaX() {
            return g.V().store("a").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX(final Object v1Id) {
            return g.V(v1Id).store("a").by("name").out().store("a").by("name").values("name").cap("a");
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX() {
            return g.withSideEffect("a", HashSetSupplier.instance()).V().both().<String>values("name").store("a").cap("a");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX_capXaX() {
            return g.V().store("a").by(outE("created").count()).out().out().store("a").by(inE("created").values("weight").sum()).cap("a");
        }

        @Override
        public Traversal<Vertex, BulkSet<String>> get_g_V_order_byXidX_storeXaX_byXnameX_out_selectXaX() {
            return g.V().order().by(T.id).store("a").by("name").out().select("a");
        }
    }
}
