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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.CrossJoinEnumerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.Enumerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.InnerJoinEnumerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.IteratorEnumerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.match.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import static org.apache.tinkerpop.gremlin.structure.P.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class MatchTest extends AbstractGremlinProcessTest {

    // very basic query
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX();

    // query with selection
    public abstract Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX();

    // linked traversals
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX();

    // a basic tree with two leaves
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__a_created_cX();

    // a tree with three leaves
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXab_nameX();

    // illustrates early deduplication in "predicate" traversals
    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_repeatXoutX_timesX2XX_selectXnameX();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXnameX();

    public abstract Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name();

    // contains a cycle
    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX();

    // contains an unreachable label
    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_knows_b__c_knows_bX();

    // nested match()
    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX();

    // contains a pair of traversals which connect the same labels, together with a predicate traversal
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX();

    // contains an identical pair of sets of traversals, up to variable names and has() conditions
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX();

    // forms a non-trivial DAG
    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX();

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX();

    // inclusion of where
    public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX();

    //TODO: with Traversal.reverse()
    //public abstract Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX();

    //TODO: with Traversal.reverse()
    // public abstract Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_out_bX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_out_bX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "vadas")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "josh")).put("b", convertToVertex(graph, "ripple")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "josh")).put("b", convertToVertex(graph, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "peter")).put("b", convertToVertex(graph, "lop")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_out_bX_selectXb_idX() throws Exception {
        final Traversal<Vertex, Object> traversal = get_g_V_matchXa_out_bX_selectXb_idX();
        printTraversalForm(traversal);
        int counter = 0;
        final Object vadasId = convertToVertexId("vadas");
        final Object joshId = convertToVertexId("josh");
        final Object lopId = convertToVertexId("lop");
        final Object rippleId = convertToVertexId("ripple");
        Map<Object, Long> idCounts = new HashMap<>();
        while (traversal.hasNext()) {
            counter++;
            MapHelper.incr(idCounts, traversal.next(), 1l);
        }
        assertFalse(traversal.hasNext());
        assertEquals(idCounts.get(vadasId), Long.valueOf(1l));
        assertEquals(idCounts.get(lopId), Long.valueOf(3l));
        assertEquals(idCounts.get(joshId), Long.valueOf(1l));
        assertEquals(idCounts.get(rippleId), Long.valueOf(1l));
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_knows_b__b_created_cX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_knows_b__b_created_cX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "ripple")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_knows_b__a_created_cX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_knows_b__a_created_cX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "vadas")).put("c", convertToVertex(graph, "lop")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "lop")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("d", convertToVertex(graph, "vadas")).put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "lop")),
                new Bindings<Vertex>().put("d", convertToVertex(graph, "vadas")).put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "ripple")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXab_nameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXab_nameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertResults(Function.identity(), traversal, new Bindings<String>().put("a", "marko").put("b", "lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXnameX() throws Exception {
        final List<Traversal<Vertex, Map<String, String>>> traversals = Arrays.asList(
                get_g_V_matchXa_created_lop_b__b_0created_29_c__c_repeatXoutX_timesX2XX_selectXnameX(),
                get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXnameX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertResults(Function.identity(), traversal,
                    new Bindings<String>().put("a", "marko").put("b", "lop").put("c", "marko"),
                    new Bindings<String>().put("a", "josh").put("b", "lop").put("c", "marko"),
                    new Bindings<String>().put("a", "peter").put("b", "lop").put("c", "marko"));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() throws Exception {
        final Traversal<Vertex, String> traversal = get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name();
        printTraversalForm(traversal);
        assertEquals("lop", traversal.next());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__b_0created_aX() {
        get_g_V_matchXa_created_b__b_0created_aX();
    }

    @Test(expected = IllegalArgumentException.class)
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_knows_b__c_knows_bX() {
        get_g_V_matchXa_knows_b__c_knows_bX();
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX();
        printTraversalForm(traversal);
        assertResults(Function.identity(), traversal,
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "josh"),
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "josh"), // expected duplicate: two paths to this solution
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "marko"),
                new Bindings<String>().put("a", "marko").put("b", "josh").put("c", "peter"));
    }

    /* TODO: this test requires Traversal.reverse()
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__c_created_bX_selectXnameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__c_created_bX_selectXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Long> countMap = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, String> bindings = traversal.next();
            // TODO: c is not being bound
            // assertEquals(3, bindings.size());
            assertEquals("lop", bindings.get("b"));
            MapHelper.incr(countMap, bindings.get("a") + ":" + bindings.get("c"), 1l);
        }
        // TODO: without 'c' binding, cant check results
        // assertEquals(Long.valueOf(1), countMap.get("marko:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("marko:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("marko:peter"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("josh:peter"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:marko"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:josh"));
        //assertEquals(Long.valueOf(1), countMap.get("peter:peter"));
        //assertEquals(countMap.size(), 9);
        //assertEquals(9, counter);
        assertFalse(traversal.hasNext());
    }
    */

    /* TODO: this test requires Traversal.reverse()
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() throws Exception {
        // TODO: Doesn't work, only bindings to 'a' in binding set.
        final Traversal<Vertex, String> traversal = get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final List<String> results = traversal.toList();
        assertEquals(2, results.size());
        assertTrue(results.contains("josh"));
        assertTrue(results.contains("vadas"));
    }
    */

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>().put("a", convertToVertex(graph, "Garcia")).put("b", convertToVertex(graph, "CREAM PUFF WAR")),
                new Bindings<Vertex>().put("a", convertToVertex(graph, "Garcia")).put("b", convertToVertex(graph, "CRYPTICAL ENVELOPMENT")));
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX() throws Exception {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX();
        printTraversalForm(traversal);
        assertResults(vertexToStr, traversal,
                new Bindings<Vertex>()
                        .put("a", convertToVertex(graph, "Garcia"))
                        .put("b", convertToVertex(graph, "I WANT TO TELL YOU"))
                        .put("c", convertToVertex(graph, "STIR IT UP"))
                        .put("d", convertToVertex(graph, "George_Harrison"))
                        .put("e", convertToVertex(graph, "Bob_Marley")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX() throws Exception {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX();
        assertResults(Function.identity(), traversal,
                new Bindings<String>().put("a", "marko").put("c", "josh"),
                new Bindings<String>().put("a", "marko").put("c", "peter"),
                new Bindings<String>().put("a", "josh").put("c", "marko"),
                new Bindings<String>().put("a", "josh").put("c", "peter"),
                new Bindings<String>().put("a", "peter").put("c", "marko"),
                new Bindings<String>().put("a", "peter").put("c", "josh"));
    }


    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX() throws Exception {
        final List<Traversal<Vertex, Map<String, Vertex>>> traversals = Arrays.asList(
                get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX(),
                get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX());

        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            assertResults(vertexToStr, traversal,
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Garcia"))
                            .put("b", convertToVertex(graph, "CREAM PUFF WAR"))
                            .put("c", convertToVertex(graph, "CREAM PUFF WAR"))
                            .put("d", convertToVertex(graph, "Garcia")),
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Garcia"))
                            .put("b", convertToVertex(graph, "CREAM PUFF WAR"))
                            .put("c", convertToVertex(graph, "CRYPTICAL ENVELOPMENT"))
                            .put("d", convertToVertex(graph, "Garcia")),
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Garcia"))
                            .put("b", convertToVertex(graph, "CRYPTICAL ENVELOPMENT"))
                            .put("c", convertToVertex(graph, "CREAM PUFF WAR"))
                            .put("d", convertToVertex(graph, "Garcia")),
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Garcia"))
                            .put("b", convertToVertex(graph, "CRYPTICAL ENVELOPMENT"))
                            .put("c", convertToVertex(graph, "CRYPTICAL ENVELOPMENT"))
                            .put("d", convertToVertex(graph, "Garcia")),
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Grateful_Dead"))
                            .put("b", convertToVertex(graph, "CANT COME DOWN"))
                            .put("c", convertToVertex(graph, "DOWN SO LONG"))
                            .put("d", convertToVertex(graph, "Garcia")),
                    new Bindings<Vertex>()
                            .put("a", convertToVertex(graph, "Grateful_Dead"))
                            .put("b", convertToVertex(graph, "THE ONLY TIME IS NOW"))
                            .put("c", convertToVertex(graph, "DOWN SO LONG"))
                            .put("d", convertToVertex(graph, "Garcia")));
        });
    }


    /* TODO: is it necessary to implement each of these traversals three times?
    @Test
    @LoadGraphWith(MODERN)
    public void testTraversalUpdater() throws Exception {
        assertBranchFactor(
                2.0,
                as("a").out("knows").as("b"),
                new SingleIterator<>(g.V(1)));

        assertBranchFactor(
                0.0,
                as("a").out("foo").as("b"),
                new SingleIterator<>(g.V(1)));

        assertBranchFactor(
                7.0,
                as("a").both().both().as("b"),
                new SingleIterator<>(g.V(1)));

        assertBranchFactor(
                0.5,
                as("a").outV().has("name", "marko").as("b"),
                g.E());
    }
    */

    @Test
    @LoadGraphWith(MODERN)
    public void testOptimization() throws Exception {
        MatchStep<Object, Object> query;
        Iterator iter;

        query = new MatchStep<>(g.V().asAdmin(), "d",
                as("d").in("knows").as("a"),
                as("d").has("name", "vadas"),
                as("a").out("knows").as("b"),
                as("b").out("created").as("c"));
        iter = g.V();
        query.optimize();
        //System.out.println(query.summarize());

        // c costs nothing (no outgoing traversals)
        assertEquals(0.0, query.findCost("c"), 0);
        // b-created->c has a cost equal to its branch factor, 1.0
        // b has only one outgoing traversal, b-created->c, so its total cost is 1.0
        assertEquals(1.0, query.findCost("b"), 0);
        // the cost of a-knows->b is its branch factor (1.0) plus the branch factor times the cost of b-created->c (1.0), so 2.0
        // a has only one outgoing traversal, a-knows->b, so its total cost is 2.0
        assertEquals(2.0, query.findCost("a"), 0);
        // the cost of d<-knows-a is its branch factor (1.0) plus the branch factor times the cost of a-knows->b (2.0), so 3.0
        // the cost of d->has(name,vadas) is its branch factor (1.0)
        // the total cost of d is the cost of its first traversal times the branch factor of the first times the cost of the second,
        //     or 3.0 + 1.0*1.0 = 4.0
        assertEquals(4.0, query.findCost("d"), 0);

        // apply the query to the graph, gathering non-trivial branch factors
        assertResults(query.solveFor(iter),
                new Bindings<>().put("d", convertToVertex(graph, "vadas")).put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "lop")),
                new Bindings<>().put("d", convertToVertex(graph, "vadas")).put("a", convertToVertex(graph, "marko")).put("b", convertToVertex(graph, "josh")).put("c", convertToVertex(graph, "ripple")));
        query.optimize();
        //System.out.println(query.summarize());

        // c still costs nothing (no outgoing traversals)
        assertEquals(0.0, query.findCost("c"), 0);
        // b-created->c still has a branch factor of 1.0, as we have put two items in (josh and vadas) and gotten two out (lop and ripple)
        // b has only one outgoing traversal, b-created->c, so its total cost is 1.0
        /* TODO: adjust and restore
        assertEquals(1.0, query.findCost("b"), 0);
        // a-knows->b now has a branch factor of 2.0 -- we put in marko and got out josh and vadas
        // the cost of a-knows->b is its branch factor (2.0) plus the branch factor times the cost of b-created->c (1.0), so 4.0
        // a has only one outgoing traversal, a-knows->b, so its total cost is 4.0
        assertEquals(4.0, query.findCost("a"), 0);
        // d<-knows-a has a branch factor of 1/6 -- we put in all six vertices and got out marko
        //     we get out marko only once, because d-name->"vadas" is tried first and rules out all but one "d"
        // the cost of d<-knows-a is its branch factor (1/6) plus the branch factor times the cost of a-knows->b (4.0), so 5/6
        // since we optimized to put the has step first (it immediately eliminates most vertices),
        //     the cost of d->has(name,vadas) is 1/6 -- we put in all six vertices and got out one
        // the total cost of d is the cost of its first traversal times the branch factor of the first times the cost of the second,
        //     or 1/6 + 1/6*5/6 = 11/36
        */
    }

    // TODO: uncomment when query cycles are supported
    /*
    @Test
    @LoadGraphWith(MODERN)
    public void testCyclicPatterns() throws Exception {
        MatchStep<Object, Object> query;
        Iterator iter;

        iter = g.V();
        query = new MatchStep<>(g.V(), "a",
                as("a").out("uses").as("b"),
                as("b").out("dependsOn").as("c"),
                as("c").in("created").as("a"));

        assertResults(query.solve(iter),
                new Bindings<>().put("a", "v[1]").put("b", "v[10]").put("c", "v[11]"));
    }
    */

    @Test
    public void testIteratorEnumerator() throws Exception {
        IteratorEnumerator<String> ie;
        final Map<String, String> map = new HashMap<>();
        BiConsumer<String, String> visitor = map::put;

        ie = new IteratorEnumerator<>("a", new LinkedList<String>() {{
            add("foo");
            add("bar");
        }}.iterator());
        assertEquals(0, ie.size());
        assertTrue(ie.visitSolution(0, visitor));
        assertEquals(1, ie.size());
        assertEquals(1, map.size());
        assertEquals("foo", map.get("a"));
        map.clear();
        assertTrue(ie.visitSolution(1, visitor));
        assertEquals(2, ie.size());
        assertEquals(1, map.size());
        assertEquals("bar", map.get("a"));
        map.clear();
        assertFalse(ie.visitSolution(2, visitor));
        assertEquals(2, ie.size());
        assertEquals(0, map.size());
        // revisit previous solutions at random
        assertTrue(ie.visitSolution(1, visitor));
        assertEquals(2, ie.size());
        assertEquals(1, map.size());
        assertEquals("bar", map.get("a"));

        // empty enumerator
        ie = new IteratorEnumerator<>("a", new LinkedList<String>() {{
        }}.iterator());
        map.clear();
        assertEquals(0, ie.size());
        assertFalse(ie.visitSolution(0, visitor));
        assertEquals(0, ie.size());
        assertEquals(0, map.size());
    }

    @Test
    public void testCrossJoin() throws Exception {
        String[] a1 = new String[]{"a", "b", "c"};
        String[] a2 = new String[]{"1", "2", "3", "4"};
        String[] a3 = new String[]{"@", "#"};

        Enumerator<String> e1 = new IteratorEnumerator<>("letter", Arrays.asList(a1).iterator());
        Enumerator<String> e2 = new IteratorEnumerator<>("number", Arrays.asList(a2).iterator());
        Enumerator<String> e3 = new IteratorEnumerator<>("punc", Arrays.asList(a3).iterator());

        Enumerator<String> e1e2 = new CrossJoinEnumerator<>(e1, e2);
        Enumerator<String> e2e1 = new CrossJoinEnumerator<>(e2, e1);
        BiConsumer<String, String> visitor = (name, value) -> {
            System.out.println("\t" + name + ":\t" + value);
        };
        Enumerator<String> e1e2e3 = new CrossJoinEnumerator<>(e1e2, e3);

        Enumerator<String> result;

        result = e1e2;
        assertEquals(12, exhaust(result));
        assertEquals(12, result.size());

        result = e2e1;
        assertEquals(12, exhaust(result));
        assertEquals(12, result.size());

        result = e1e2e3;
        assertEquals(24, exhaust(result));
        assertEquals(24, result.size());

        /*
        int i = 0;
        while (result.visitSolution(i, visitor)) {
            System.out.println("solution #" + (i + 1) + "^^");
            i++;
        }
        */
    }

    /* TODO: uncomment when optimized cross-joins are available
    @Test
    public void testCrossJoinLaziness() throws Exception {
        List<Integer> value = new LinkedList<>();
        for (int j = 0; j < 1000; j++) {
            value.add(j);
        }

        int base = 3;
        List<Enumerator<Integer>> enums = new LinkedList<>();
        for (int k = 0; k < base; k++) {
            List<Integer> lNew = new LinkedList<>();
            lNew.addAll(value);
            Enumerator<Integer> ek = new IteratorEnumerator<>("" + (char) ('a' + k), lNew.iterator());
            enums.add(ek);
        }

        Enumerator<Integer> e = new NewCrossJoinEnumerator<>(enums);

        // we now have an enumerator of 5^3^10 elements
        EnumeratorIterator<Integer> iter = new EnumeratorIterator<>(e);

        int count = 0;
        // each binding set is unique
        Set<String> values = new HashSet<>();
        String s;
        s = iter.next().toString();
        values.add(s);
        assertEquals(++count, values.size());
        // begin at the head of all iterators
        assertEquals("{a=0, b=0, c=0, d=0, e=0, f=0, g=0, h=0, i=0, j=0}", s);
        int lim0 = (int) Math.pow(1, base);
        // first 2^10 results are binary (0's and 1's)
        int lim1 = (int) Math.pow(2, base);
        for (int i = lim0; i < lim1; i++) {
            s = iter.next().toString();
            System.out.println("" + i + ": " + count + ": " + s); System.out.flush();
            assertTrue(s.contains("1"));
            assertFalse(s.contains("2"));
            values.add(s);
            assertEquals(++count, values.size());
        }
        int lim2 = (int) Math.pow(3, base);
        for (int i = lim1; i < lim2; i++) {
            s = iter.next().toString();
            System.out.println("" + i + ": " + count + ": " + s); System.out.flush();

            if (!s.contains("2")) {
                findMissing(null, 0, base, "abcdefghij".getBytes(), values);
            }


            assertTrue(s.contains("2"));
            assertFalse(s.contains("3"));
            values.add(s);
            assertEquals(++count, values.size());
        }
    }
    */

    @Test
    public void testInnerJoin() throws Exception {
        String[] a1 = new String[]{"a", "b", "c"};
        String[] a2 = new String[]{"1", "2", "3", "4"};
        String[] a3 = new String[]{"2", "4", "6", "8", "10"};

        Enumerator<String> e1 = new IteratorEnumerator<>("letter", Arrays.asList(a1).iterator());
        Enumerator<String> e2 = new IteratorEnumerator<>("number", Arrays.asList(a2).iterator());
        Enumerator<String> e3 = new IteratorEnumerator<>("number", Arrays.asList(a3).iterator());

        Enumerator<String> e4 = new CrossJoinEnumerator<>(e1, e3);
        Enumerator<String> e5 = new CrossJoinEnumerator<>(e2, e4);

        // without AND semantics, we have all 60 combinations, including two "number" bindings per solution
        exhaust(e5);
        assertEquals(60, e5.size());

        // with AND semantics, we have only those six solutions for which a2 and a3 align
        Enumerator<String> join = new InnerJoinEnumerator<>(e5, new HashSet<String>() {{
            add("number");
        }});
        exhaust(join);
        assertEquals(6, join.size());

        assertResults(join,
                new Bindings<String>().put("letter", "a").put("number", "2"),
                new Bindings<String>().put("letter", "a").put("number", "4"),
                new Bindings<String>().put("letter", "b").put("number", "2"),
                new Bindings<String>().put("letter", "b").put("number", "4"),
                new Bindings<String>().put("letter", "c").put("number", "2"),
                new Bindings<String>().put("letter", "c").put("number", "4"));
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class Traversals extends MatchTest {
        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            return g.V().match("a", as("a").out().as("b"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            return g.V().match("a", as("a").out().as("b")).select("b").by(T.id);
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            return g.V().match("a",
                    as("a").out("knows").as("b"),
                    as("b").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__a_created_cX() {
            return g.V().match("a",
                    as("a").out("knows").as("b"),
                    as("a").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX() {
            return g.V().match("d",
                    as("d").in("knows").as("a"),
                    as("d").has("name", "vadas"),
                    as("a").out("knows").as("b"),
                    as("b").out("created").as("c"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXab_nameX() {
            return g.V().match("a",
                    as("a").out("created").as("b"),
                    __.<Vertex>as("a").repeat(out()).times(2).as("b")).<String>select("a", "b").by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_repeatXoutX_timesX2XX_selectXnameX() {
            return g.V().match("a",
                    as("a").out("created").has("name", "lop").as("b"),
                    as("b").in("created").has("age", 29).as("c"),
                    __.<Vertex>as("c").repeat(out()).times(2)).<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXnameX() {
            return g.V().match("a",
                    as("a").out("created").has("name", "lop").as("b"),
                    as("b").in("created").has("age", 29).as("c"))
                    .where(__.<Vertex>as("c").repeat(out()).times(2)).<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            return g.V().out().out().match("a",
                    as("a").in("created").as("b"),
                    as("b").in("knows").as("c")).select("c").out("created").values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX() {
            return g.V().match("a",
                    as("a").out("created").as("b"),
                    as("b").in("created").as("a"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_knows_b__c_knows_bX() {
            return g.V().match("a", as("a").out("knows").as("b"),
                    as("c").out("knows").as("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
            return g.V().match("a",
                    as("a").out("knows").as("b"),
                    as("b").out("created").has("name", "lop"),
                    as("b").match("a1",
                            as("a1").out("created").as("b1"),
                            as("b1").in("created").as("c1")).select("c1").as("c")).<String>select().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX() {
            return g.V().match("a",
                    as("a").has("name", "Garcia"),
                    as("a").in("writtenBy").as("b"),
                    as("a").in("sungBy").as("b"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX() {
            return g.V().match("a",
                    as("a").in("sungBy").as("b"),
                    as("a").in("sungBy").as("c"),
                    as("b").out("writtenBy").as("d"),
                    as("c").out("writtenBy").as("e"),
                    as("d").has("name", "George_Harrison"),
                    as("e").has("name", "Bob_Marley"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX() {
            return g.V().match("a",
                    as("a").in("sungBy").as("b"),
                    as("a").in("writtenBy").as("c"),
                    as("b").out("writtenBy").as("d"),
                    as("c").out("sungBy").as("d"),
                    as("d").has("name", "Garcia"));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX() {
            return g.V().match("a",
                    as("a").in("sungBy").as("b"),
                    as("a").in("writtenBy").as("c"),
                    as("b").out("writtenBy").as("d"))
                    .where(as("c").out("sungBy").as("d"))
                    .where(as("d").has("name", "Garcia"));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX() {
            return g.V().match("a",
                    as("a").out("created").as("b"),
                    as("b").in("created").as("c"))
                    .where("a", neq("c"))
                    .<String>select("a", "c").by("name");
        }

        /*@Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
            return g.V().match("a",
                    as("a").out("created").as("b"),
                    as("c").out("created").as("b")).select(v -> ((Vertex) v).value("name"));
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
            return g.V().out().out().match("a",
                    as("b").out("created").as("a"),
                    as("c").out("knows").as("b")).select("c").out("knows").value("name");
        }*/

    }

    private static final Function<Vertex, String> vertexToStr = v -> v.id().toString();

    private <S, E> void assertResults(final Function<E, String> toStringFunction,
                                      final Traversal<S, Map<String, E>> actual,
                                      final Bindings<E>... expected) {
        Comparator<Bindings<E>> comp = new Bindings.BindingsComparator<>(toStringFunction);

        List<Bindings<E>> actualList = toBindings(actual);
        List<Bindings<E>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        } else if (actualList.size() > expectedList.size()) {
            fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
        }

        Collections.sort(actualList, comp);
        Collections.sort(expectedList, comp);

        for (int j = 0; j < actualList.size(); j++) {
            Bindings<E> a = actualList.get(j);
            Bindings<E> e = expectedList.get(j);

            if (0 != comp.compare(a, e)) {
                fail("unexpected result(s), including " + a);
            }
        }
        assertFalse(actual.hasNext());
    }

    private static final Function<Object, String> simpleToStringFunction = Object::toString;

    private <T> void assertResults(final Enumerator<T> actual,
                                   final Bindings<T>... expected) {
        Comparator<Bindings<T>> comp = new Bindings.BindingsComparator<>((Function<T, String>) simpleToStringFunction);

        List<Bindings<T>> actualList = toBindings(actual);
        List<Bindings<T>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        Collections.sort(actualList, comp);
        Collections.sort(expectedList, comp);

        for (int j = 0; j < actualList.size(); j++) {
            if (j == expectedList.size()) {
                fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
            }

            Bindings<T> a = actualList.get(j);
            Bindings<T> e = expectedList.get(j);

            if (0 != comp.compare(a, e)) {
                fail("unexpected result(s), including " + a);
            }
        }
        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        }
    }

    private <S, E> List<Bindings<E>> toBindings(final Traversal<S, Map<String, E>> traversal) {
        List<Bindings<E>> result = new LinkedList<>();
        traversal.forEachRemaining(o -> {
            result.add(new Bindings<>(o));
        });
        return result;
    }

    private <T> List<Bindings<T>> toBindings(final Enumerator<T> enumerator) {
        List<Bindings<T>> bindingsList = new LinkedList<>();
        int i = 0;
        bindingsList.add(new Bindings<>());
        while (enumerator.visitSolution(i++, (name, value) -> {
            bindingsList.get(bindingsList.size() - 1).put(name, value);
        })) {
            bindingsList.add(new Bindings<>());
        }
        bindingsList.remove(bindingsList.size() - 1);

        return bindingsList;
    }

    private void assertBranchFactor(final double branchFactor,
                                    final Traversal t,
                                    final Iterator inputs) {
        Traverser start = new B_O_S_SE_SL_Traverser(null, null, 1l);  // TODO bad? P?
        MatchStep.TraversalWrapper w = new MatchStep.TraversalWrapper(t, "a", "b");
        MatchStep.TraversalUpdater updater = new MatchStep.TraversalUpdater<>(w, inputs, start, "x");
        while (updater.hasNext()) {
            updater.next();
        }
        assertEquals(branchFactor, w.findBranchFactor(), 0);
    }

    private <T> int exhaust(Enumerator<T> enumerator) {
        BiConsumer<String, T> visitor = (s, t) -> {
        };
        int i = 0;
        while (enumerator.visitSolution(i, visitor)) i++;
        return i;
    }

    private void findMissing(String s, final int i, final int n, final byte[] names, final Set<String> actual) {
        if (0 == i) {
            s = "{";
        } else {
            s += ", ";
        }

        s += (char) names[i];
        s += "=";
        String tmp = s;
        for (int j = 0; j < 3; j++) {
            s += j;

            if (n - 1 == i) {
                s += "}";
                if (!actual.contains(s)) {
                    fail("not in set: " + s);
                }
            } else {
                findMissing(s, i + 1, n, names, actual);
            }

            s = tmp;
        }
    }
}
