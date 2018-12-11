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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AggregateTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX();

    public abstract Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX();

    public abstract Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path();

    public abstract Traversal<Vertex, Collection<Integer>> get_g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX();

    //public abstract Traversal<Vertex, Path> get_g_v1_asXxX_bothE_asXeX_valueXweightX_exceptXwX_aggregateXwX_backXeX_otherV_jumpXx_true_trueX_path(final Object v1Id);

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

    /*@Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_asXxX_bothE_asXeX_valueXweightX_exceptXwX_aggregateXwX_backXeX_otherV_jumpXx_true_trueX_path() {
        Iterator<Path> traversal = get_g_v1_asXxX_bothE_asXeX_valueXweightX_exceptXwX_aggregateXwX_backXeX_otherV_jumpXx_true_trueX_path(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        final List<Path> paths = StreamFactory.stream(traversal).collect(Collectors.toList());
        // for OLTP it's a roulette game; the result can change depending on which path is taken first by the traverser (this makes some cool real world use cases possible)
        // Senzari use case: generate a random playlist without artist repetitions
        assertEquals(4, paths.size());
        assertEquals(3, paths.stream().filter(path -> path.size() == 3).count());
        assertEquals(1, paths.stream().filter(path -> path.size() == 5).count());
        assertFalse(traversal.hasNext());
    }*/


    public static class Traversals extends AggregateTest {

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

        /*public Traversal<Vertex, Path> get_g_v1_asXxX_bothE_asXeX_valueXweightX_exceptXwX_aggregateXwX_backXeX_otherV_jumpXx_true_trueX_path(final Object v1Id) {
            return g.V(1).as("x").bothE().as("e").value("weight").except("w").aggregate("w").back("e").otherV().jump("x", t -> true, t -> true).path();
        }*/
    }
}
