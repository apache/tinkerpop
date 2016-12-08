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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.group;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.project;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ComplexTest extends AbstractGremlinProcessTest {

    /**
     * Checks the result of both coworkerSummary tests, which is expected to look as follows:
     * <p>
     * marko={peter={numCoCreated=1, coCreated=[lop]}, josh={numCoCreated=1, coCreated=[lop]}}
     * josh={peter={numCoCreated=1, coCreated=[lop]}, marko={numCoCreated=1, coCreated=[lop]}}
     * peter={josh={numCoCreated=1, coCreated=[lop]}, marko={numCoCreated=1, coCreated=[lop]}}
     */
    private static void checkCoworkerSummary(final Map<String, Map<String, Map<String, Object>>> summary) {
        assertNotNull(summary);
        assertEquals(3, summary.size());
        assertTrue(summary.containsKey("marko"));
        assertTrue(summary.containsKey("josh"));
        assertTrue(summary.containsKey("peter"));
        for (final Map.Entry<String, Map<String, Map<String, Object>>> entry : summary.entrySet()) {
            assertEquals(2, entry.getValue().size());
            switch (entry.getKey()) {
                case "marko":
                    assertTrue(entry.getValue().containsKey("josh") && entry.getValue().containsKey("peter"));
                    break;
                case "josh":
                    assertTrue(entry.getValue().containsKey("peter") && entry.getValue().containsKey("marko"));
                    break;
                case "peter":
                    assertTrue(entry.getValue().containsKey("marko") && entry.getValue().containsKey("josh"));
                    break;
            }
            for (final Map<String, Object> m : entry.getValue().values()) {
                assertTrue(m.containsKey("numCoCreated"));
                assertTrue(m.containsKey("coCreated"));
                assertTrue(m.get("numCoCreated") instanceof Number);
                assertTrue(m.get("coCreated") instanceof Collection);
                assertEquals(1, ((Number) m.get("numCoCreated")).intValue());
                assertEquals(1, ((Collection) m.get("coCreated")).size());
                assertEquals("lop", ((Collection) m.get("coCreated")).iterator().next());
            }
        }
    }

    public abstract Traversal<Vertex, String> getClassicRecommendation();

    public abstract Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> getCoworkerSummary();

    public abstract Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> getCoworkerSummaryOLTP();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    public void classicRecommendation() {
        final Traversal<Vertex, String> traversal = getClassicRecommendation();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("LET IT GROW", "UNCLE JOHNS BAND", "I KNOW YOU RIDER", "SHIP OF FOOLS", "GOOD LOVING"), traversal);
        final List<Map<String, Object>> list = new ArrayList<>(traversal.asAdmin().getSideEffects().<BulkSet<Map<String, Object>>>get("m"));
        assertEquals(5, list.size());
        assertFalse(traversal.hasNext());
        assertEquals("LET IT GROW", list.get(0).get("x"));
        assertEquals(276, list.get(0).get("y"));
        assertEquals(21L, list.get(0).get("z"));
        //
        assertEquals("UNCLE JOHNS BAND", list.get(1).get("x"));
        assertEquals(332, list.get(1).get("y"));
        assertEquals(20L, list.get(1).get("z"));
        //
        assertEquals("I KNOW YOU RIDER", list.get(2).get("x"));
        assertEquals(550, list.get(2).get("y"));
        assertEquals(20L, list.get(2).get("z"));
        //
        assertEquals("SHIP OF FOOLS", list.get(3).get("x"));
        assertEquals(225, list.get(3).get("y"));
        assertEquals(18L, list.get(3).get("z"));
        //
        assertEquals("GOOD LOVING", list.get(4).get("x"));
        assertEquals(428, list.get(4).get("y"));
        assertEquals(18L, list.get(4).get("z"));
        //
        checkSideEffects(traversal.asAdmin().getSideEffects(), "m", BulkSet.class, "stash", BulkSet.class);

    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void coworkerSummary() {
        final Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal = getCoworkerSummary();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        checkCoworkerSummary(traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER) // no mid-traversal V() in computer mode + star-graph limitations
    public void coworkerSummaryOLTP() {
        final Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal = getCoworkerSummaryOLTP();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        checkCoworkerSummary(traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends ComplexTest {

        @Override
        public Traversal<Vertex, String> getClassicRecommendation() {
            return g.V().has("name", "DARK STAR").as("a").out("followedBy").aggregate("stash").
                    in("followedBy").where(neq("a").and(P.not(P.within("stash")))).
                    groupCount().
                    unfold().
                    project("x", "y", "z").
                    by(select(Column.keys).values("name")).
                    by(select(Column.keys).values("performances")).
                    by(select(Column.values)).
                    order().
                    by(select("z"), Order.decr).
                    by(select("y"), Order.incr).
                    limit(5).store("m").select("x");
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> getCoworkerSummary() {
            return g.V().hasLabel("person").filter(outE("created")).aggregate("p").as("p1").values("name").as("p1n")
                    .select("p").unfold().where(neq("p1")).as("p2").values("name").as("p2n").select("p2")
                    .out("created").choose(in("created").where(eq("p1")), values("name"), constant(emptyList()))
                    .<String, Map<String, Map<String, Object>>>group().by(select("p1n")).
                            by(group().by(select("p2n")).
                                    by(unfold().fold().project("numCoCreated", "coCreated").by(count(local)).by()));
        }

        @Override
        public Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> getCoworkerSummaryOLTP() {
            return g.V().hasLabel("person").filter(outE("created")).as("p1")
                    .V().hasLabel("person").where(neq("p1")).filter(outE("created")).as("p2")
                    .map(out("created").where(in("created").as("p1")).values("name").fold())
                    .<String, Map<String, Map<String, Object>>>group().by(select("p1").by("name")).
                            by(group().by(select("p2").by("name")).
                                    by(project("numCoCreated", "coCreated").by(count(local)).by()));
        }
    }
}

