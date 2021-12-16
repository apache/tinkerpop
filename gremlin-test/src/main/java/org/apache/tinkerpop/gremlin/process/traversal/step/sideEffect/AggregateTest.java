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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AggregateTest extends AbstractGremlinProcessTest {

    ////// global

    public abstract Traversal<Vertex, List<String>> get_g_V_name_aggregateXglobal_xX_capXxX();

    public abstract Traversal<Vertex, List<Integer>> get_g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX();

    public abstract Traversal<Vertex, List<Vertex>> get_g_V_aggregateXxX_byXout_order_byXnameXX_capXxX();

    public abstract Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX();

    public abstract Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX();

    public abstract Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path();

    public abstract Traversal<Vertex, Collection<Integer>> get_g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX();

    ////// local

    public abstract Traversal<Vertex, Collection> get_g_V_aggregateXlocal_aX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Collection> get_g_VX1X_aggregateXlocal_aX_byXnameX_out_aggregateXlocal_aX_byXnameX_name_capXaX(final Object v1Id);

    public abstract Traversal<Vertex, Set<String>> get_g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX();

    public abstract Traversal<Vertex, Collection> get_g_V_aggregateXlocal_aX_byXoutEXcreatedX_countX_out_out_aggregateXlocal_aX_byXinEXcreatedX_weight_sumX_capXaX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueXnameX_aggregateXglobal_xX_capXxX() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_name_aggregateXglobal_xX_capXxX();
        printTraversalForm(traversal);
        final Collection<String> names = traversal.next();
        assertFalse(traversal.hasNext());
        checkListOfNames(names);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueXnameX_aggregateXxX_capXxX() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_name_aggregateXxX_capXxX();
        printTraversalForm(traversal);
        final Collection<String> names = traversal.next();
        assertFalse(traversal.hasNext());
        checkListOfNames(names);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXxX_byXnameX_capXxX() {
        final Traversal<Vertex, List<String>> traversal = get_g_V_aggregateXxX_byXnameX_capXxX();
        printTraversalForm(traversal);
        final Collection<String> names = traversal.next();
        assertFalse(traversal.hasNext());
        checkListOfNames(names);
    }

    private void checkListOfNames(Collection<String> names) {
        assertEquals(6, names.size());
        assertTrue(names.contains("marko"));
        assertTrue(names.contains("josh"));
        assertTrue(names.contains("peter"));
        assertTrue(names.contains("lop"));
        assertTrue(names.contains("vadas"));
        assertTrue(names.contains("ripple"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_aggregateXaX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_out_aggregateXaX_path();
        printTraversalForm(traversal);
        int count = 0;
        final Map<String, Long> firstStepCounts = new HashMap<>();
        final Map<String, Long> secondStepCounts = new HashMap<>();
        while (traversal.hasNext()) {
            count++;
            final Path path = traversal.next();
            final String first = path.get(0).toString();
            final String second = path.get(1).toString();
            assertThat(first, not(second));
            MapHelper.incr(firstStepCounts, first, 1l);
            MapHelper.incr(secondStepCounts, second, 1l);
        }
        assertEquals(6, count);
        assertEquals(3, firstStepCounts.size());
        assertEquals(4, secondStepCounts.size());
        assertTrue(firstStepCounts.values().contains(3l));
        assertTrue(firstStepCounts.values().contains(2l));
        assertTrue(firstStepCounts.values().contains(1l));
        assertTrue(secondStepCounts.values().contains(3l));
        assertTrue(secondStepCounts.values().contains(1l));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX() {
        final Traversal<Vertex, Collection<Integer>> traversal = get_g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX();
        final Collection<Integer> ages = traversal.next();

        // in 3.3.x a BulkSet will coerce to List under GraphSON
        assumeThat(ages instanceof BulkSet, is(true));
        assertEquals(4, ages.size());
        assertTrue(ages.contains(29));
        assertTrue(ages.contains(27));
        assertTrue(ages.contains(32));
        assertTrue(ages.contains(35));
        final BulkSet<Integer> bulkSet = new BulkSet<>();
        bulkSet.add(29);
        bulkSet.add(27);
        bulkSet.add(32);
        bulkSet.add(35);
        assertEquals(bulkSet, ages); // ensure bulk set equality
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXlocal_a_nameX_out_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_V_aggregateXlocal_aX_byXnameX_out_capXaX();
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
    public void g_VX1X_aggregateXlocal_aX_byXnameX_out_aggregateXlocal_aX_byXnameX_name_capXaX() {
        final Traversal<Vertex, Collection> traversal = get_g_VX1X_aggregateXlocal_aX_byXnameX_out_aggregateXlocal_aX_byXnameX_name_capXaX(convertToVertexId("marko"));
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
    public void g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX() {
        final Traversal<Vertex, Set<String>> traversal = get_g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX();
        printTraversalForm(traversal);
        final Set<String> names = traversal.next();
        assertFalse(traversal.hasNext());
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
    public void g_V_aggregateXlocal_aX_byXoutEXcreatedX_countX_out_out_aggregateXlocal_aX_byXinEXcreatedX_weight_sumX() {
        final Traversal<Vertex, Collection> traversal = get_g_V_aggregateXlocal_aX_byXoutEXcreatedX_countX_out_out_aggregateXlocal_aX_byXinEXcreatedX_weight_sumX_capXaX();
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
    public void g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX() {
        final Traversal<Vertex, List<Integer>> traversal = get_g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX();
        printTraversalForm(traversal);
        final Collection<Integer> ages = traversal.next();
        assertFalse(traversal.hasNext());
        assertThat(ages, containsInAnyOrder(32, 35));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXxX_byXout_order_byXnameXX_capXxX() {
        final Traversal<Vertex, List<Vertex>> traversal = get_g_V_aggregateXxX_byXout_order_byXnameXX_capXxX();
        printTraversalForm(traversal);
        final Collection<Vertex> ages = traversal.next();
        assertFalse(traversal.hasNext());
        assertThat(ages, containsInAnyOrder(convertToVertex("lop"), convertToVertex("josh"), convertToVertex("lop")));
    }

    public static class Traversals extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregateXglobal_xX_capXxX() {
            return g.V().values("name").aggregate(Scope.global, "x").cap("x");
        }

        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_aggregateXxX_byXout_order_byXnameXX_capXxX() {
            return g.V().aggregate("x").by(__.out().order().by("name")).cap("x");
        }

        @Override
        public Traversal<Vertex, List<Integer>> get_g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX() {
            return g.V().aggregate("x").by(__.values("age").is(P.gt(29))).cap("x");
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX() {
            return g.V().values("name").aggregate("x").cap("x");
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX() {
            return g.V().aggregate("x").by("name").cap("x");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            return g.V().out().aggregate("a").path();
        }

        @Override
        public Traversal<Vertex, Collection<Integer>> get_g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX() {
            return g.V().hasLabel("person").aggregate("x").by("age").cap("x").as("y").select("y");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_aggregateXlocal_aX_byXnameX_out_capXaX() {
            return g.V().aggregate(Scope.local, "a").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_VX1X_aggregateXlocal_aX_byXnameX_out_aggregateXlocal_aX_byXnameX_name_capXaX(final Object v1Id) {
            return g.V(v1Id).aggregate(Scope.local, "a").by("name").out().aggregate(Scope.local, "a").by("name").values("name").cap("a");
        }

        @Override
        public Traversal<Vertex, Set<String>> get_g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX() {
            return g.withSideEffect("a", new HashSet()).V().both().<String>values("name").aggregate(Scope.local, "a").cap("a");
        }

        @Override
        public Traversal<Vertex, Collection> get_g_V_aggregateXlocal_aX_byXoutEXcreatedX_countX_out_out_aggregateXlocal_aX_byXinEXcreatedX_weight_sumX_capXaX() {
            return g.V().aggregate(Scope.local, "a").by(outE("created").count()).out().out().aggregate(Scope.local, "a").by(inE("created").values("weight").sum()).cap("a");
        }
    }
}
