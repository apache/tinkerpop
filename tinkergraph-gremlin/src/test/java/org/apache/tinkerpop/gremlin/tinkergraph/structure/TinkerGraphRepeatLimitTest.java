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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.util.List;
import java.util.function.Function;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TinkerGraphRepeatLimitTest {
    private static GraphTraversalSource gSource;
    private static GraphTraversalSource gComputerSource;

    @BeforeClass
    public static void setup() {
        gSource = TinkerGraph.open().traversal().withoutStrategies(RepeatUnrollStrategy.class);
        load(gSource);
        gComputerSource = gSource.withComputer(Computer.compute().workers(3));
    }

    @Test
    public void testBasicRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(1).out("knows"))
                        .times(2),
                s -> s.V().has("id", "l1-0")
                        .limit(1).out("knows")
                        .limit(1).out("knows"));
    }

    @Test
    public void testBasicRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(2).out("knows"))
                        .times(2),
                s -> s.V().has("id", "l1-0")
                        .limit(2).out("knows")
                        .limit(2).out("knows"));

    }

    @Test
    public void testRepeatOutLimit() {
        testTraversals(s -> s.V().has("id", "l2-0")
                        // adding order inside repeat breaks things
                        .repeat(__.out("knows").limit(2))
                        .times(2),
                s -> s.V().has("id", "l2-0")
                        .out("knows").limit(2)
                        .out("knows").limit(2));
    }

    @Test
    public void testRepeatWithBothAndLimit() {
        testTraversals(s -> s.V().has("id", "l3-0")
                        .repeat(__.both("knows").order().by("id").limit(3))
                        .times(2),
                s -> s.V().has("id", "l3-0")
                        .both("knows").order().by("id").limit(3)
                        .both("knows").order().by("id").limit(3));
    }

    @Test
    public void testRepeatWithFilterAndLimit() {
        testTraversals(s -> s.V().has("id", "l2-0")
                        .repeat(__.out("knows").has("id", P.neq("l4-3")).order().limit(2))
                        .times(2),
                s -> s.V().has("id", "l2-0")
                        .out("knows").has("id", P.neq("l4-3")).order().limit(2)
                        .out("knows").has("id", P.neq("l4-3")).order().limit(2));
    }

    @Test
    public void testChainedRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l2-0")
                        .repeat(__.order().by("id").limit(1).out("knows")).times(2)
                        .repeat(__.order().by("id").limit(1).in("knows")).times(2),
                s -> s.V().has("id", "l2-0")
                        .order().by("id").limit(1).out("knows")
                        .order().by("id").limit(1).out("knows")
                        .order().by("id").limit(1).in("knows")
                        .order().by("id").limit(1).in("knows"));
    }

    @Test
    public void testChainedRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l2-0")
                        .repeat(__.order().by("id").limit(2).out("knows")).times(2)
                        .repeat(__.order().by("id").limit(3).in("knows")).times(2),
                s -> s.V().has("id", "l2-0")
                        .order().by("id").limit(2).out("knows")
                        .order().by("id").limit(2).out("knows")
                        .order().by("id").limit(3).in("knows")
                        .order().by("id").limit(3).in("knows"));
    }

    @Test
    public void testNestedRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l3-0")
                .repeat(__.order().by("id").limit(1).out("knows")
                        .repeat(__.order().by("id").limit(1).in("knows"))
                        .times(2))
                .times(2), s -> s.V().has("id", "l3-0")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows"));
    }

    @Test
    public void testNestedRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l3-0")
                .repeat(__.order().by("id").limit(2).out("knows")
                        .repeat(__.order().by("id").limit(3).in("knows"))
                        .times(2))
                .times(2), s -> s.V().has("id", "l3-0")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(3).in("knows"));
    }

    @Test
    public void testTripleNestedRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(1).out("knows")
                                .repeat(__.limit(1).in("knows")
                                        .repeat(__.limit(1).out("knows"))
                                        .times(2))
                                .times(2))
                        .times(2),
                s -> s.V().has("id", "l1-0")
                        .limit(1).out("knows")
                        .limit(1).in("knows")
                        .limit(1).out("knows")
                        .limit(1).out("knows")
                        .limit(1).in("knows")
                        .limit(1).out("knows")
                        .limit(1).out("knows")
                        .limit(1).out("knows")
                        .limit(1).in("knows")
                        .limit(1).out("knows")
                        .limit(1).out("knows")
                        .limit(1).in("knows")
                        .limit(1).out("knows")
                        .limit(1).out("knows"));
    }

    @Test
    public void testTripleNestedRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(2).out("knows")
                                .repeat(__.limit(3).in("knows")
                                        .repeat(__.limit(4).out("knows"))
                                        .times(2))
                                .times(2))
                        .times(2),
                s -> s.V().has("id", "l1-0")
                        .limit(2).out("knows")
                        .limit(3).in("knows")
                        .limit(4).out("knows")
                        .limit(4).out("knows")
                        .limit(3).in("knows")
                        .limit(4).out("knows")
                        .limit(4).out("knows")
                        .limit(2).out("knows")
                        .limit(3).in("knows")
                        .limit(4).out("knows")
                        .limit(4).out("knows")
                        .limit(3).in("knows")
                        .limit(4).out("knows")
                        .limit(4).out("knows"));
    }

    @Test
    public void testAggregateRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(1).out("knows").aggregate("x"))
                        .times(2)
                        .cap("x"),
                s -> s.V().has("id", "l1-0")
                        .limit(1).out("knows").aggregate("x")
                        .limit(1).out("knows").aggregate("x")
                        .cap("x"));
    }

    @Test
    public void testAggregateRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .repeat(__.limit(3).out("knows").aggregate("x"))
                        .times(2)
                        .cap("x"),
                s -> s.V().has("id", "l1-0")
                        .limit(3).out("knows").aggregate("x")
                        .limit(3).out("knows").aggregate("x")
                        .cap("x"));
    }

    @Test
    public void testUnionRepeatLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .union(out().limit(1), out().out().limit(1))
                        .repeat(__.limit(1)).times(1),
                s -> s.V().has("id", "l1-0")
                        .union(out().limit(1), out().out().limit(1))
                        .limit(1));
    }

    @Test
    public void testUnionRepeatLimitIncreasedLimit() {
        testTraversals(s -> s.V().has("id", "l1-0")
                        .union(out().limit(1), out().out().limit(1))
                        .repeat(__.limit(3)).times(1),
                s -> s.V().has("id", "l1-0")
                        .union(out().limit(1), out().out().limit(1))
                        .limit(3));
    }

    private List<?> toListAndPrint(String header, GraphTraversal<?, ?> t) {
        System.out.println("=====" + header + "===================================");
        System.out.println(t);
        List<?> list = t.toList();
        for (Object o : list) {
            System.out.println(o);
        }
        return list;
    }

    private void testTraversals(Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> repeatFunction,
                                Function<GraphTraversalSource, GraphTraversal<Vertex, Vertex>> nonRepeatFunction) {
        List<?> repeatTraversal = toListAndPrint("repeatTraversal", repeatFunction.apply(gSource));
        List<?> withComputerTraversal = toListAndPrint("withComputerTraversal", repeatFunction.apply(gComputerSource));
        List<?> nonRepeatTraversal = toListAndPrint("nonRepeatTraversal", nonRepeatFunction.apply(gSource));
        assertFalse(repeatTraversal.isEmpty());
        assertTrue(CollectionUtils.isEqualCollection(repeatTraversal, nonRepeatTraversal));
        // due to non-sequential processing of computer algorithm, can not assert equality but only size
        assertEquals(repeatTraversal.size(), withComputerTraversal.size());
    }
    
    private static void load(GraphTraversalSource g) {
        g.addV("node").property("id", "l1-0").iterate();

        g.addV("node").property("id", "l2-0").iterate();
        g.addV("node").property("id", "l2-1").iterate();

        g.addV("node").property("id", "l3-0").iterate();
        g.addV("node").property("id", "l3-1").iterate();
        g.addV("node").property("id", "l3-2").iterate();

        g.addV("node").property("id", "l4-0").iterate();
        g.addV("node").property("id", "l4-1").iterate();
        g.addV("node").property("id", "l4-2").iterate();
        g.addV("node").property("id", "l4-3").iterate();

        g.addV("node").property("id", "l5-0").iterate();
        g.addV("node").property("id", "l5-1").iterate();
        g.addV("node").property("id", "l5-2").iterate();
        g.addV("node").property("id", "l5-3").iterate();
        g.addV("node").property("id", "l5-4").iterate();

        g.V().has("id", "l1-0").as("n1").V().has("id", "l2-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l1-0").as("n1").V().has("id", "l2-1").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-2").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l1-0").as("n1").V().has("id", "l2-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l1-0").as("n1").V().has("id", "l2-1").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l2-0").as("n1").V().has("id", "l3-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l2-1").as("n1").V().has("id", "l3-2").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-0").as("n1").V().has("id", "l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-1").as("n1").V().has("id", "l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l3-2").as("n1").V().has("id", "l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-0").as("n1").V().has("id", "l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-1").as("n1").V().has("id", "l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-2").as("n1").V().has("id", "l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l4-3").as("n1").V().has("id", "l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();

        // Add layer 6 nodes
        g.addV("node").property("id", "l6-0").iterate();
        g.addV("node").property("id", "l6-1").iterate();
        g.addV("node").property("id", "l6-2").iterate();
        g.addV("node").property("id", "l6-3").iterate();
        g.addV("node").property("id", "l6-4").iterate();
        g.addV("node").property("id", "l6-5").iterate();

        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        // Connect layer 5 to layer 6 with "friend" edges (same pattern)
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-0").as("n1").V().has("id", "l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-1").as("n1").V().has("id", "l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-2").as("n1").V().has("id", "l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-3").as("n1").V().has("id", "l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l5-4").as("n1").V().has("id", "l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        // Add layer 7 nodes
        g.addV("node").property("id", "l7-0").iterate();
        g.addV("node").property("id", "l7-1").iterate();
        g.addV("node").property("id", "l7-2").iterate();
        g.addV("node").property("id", "l7-3").iterate();
        g.addV("node").property("id", "l7-4").iterate();
        g.addV("node").property("id", "l7-5").iterate();
        g.addV("node").property("id", "l7-6").iterate();

        // Connect layer 6 to layer 7 with "knows" edges
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        // Connect layer 6 to layer 7 with "friend" edges
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-0").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-1").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-2").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-3").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-4").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id", "l6-5").as("n1").V().has("id", "l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();
    }
}
