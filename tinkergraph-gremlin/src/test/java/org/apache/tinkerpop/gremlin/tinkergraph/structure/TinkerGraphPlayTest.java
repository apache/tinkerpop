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

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ConsoleMutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLIo;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.choose;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sack;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.union;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com);
 */
public class TinkerGraphPlayTest {
    private static final Logger logger = LoggerFactory.getLogger(TinkerGraphPlayTest.class);
    private static GraphTraversalSource g;
    
    @BeforeClass
    public static void setup() {
        g = TinkerGraph.open().traversal().withoutStrategies(RepeatUnrollStrategy.class);
        load(g);
//        g = g.withComputer(Computer.compute().workers(1));
    }
    
    @Test
    public void testBasicRepeatLimit() {
        GraphTraversal<Vertex, Vertex> basic = g.V().has("id", "l1-0")
                .repeat(__.limit(1).out("knows"))
                .times(2);
        GraphTraversal<Vertex, Vertex> basicUnfolded = g.V().has("id", "l1-0")
                .limit(1).out("knows")
                .limit(1).out("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("basic", basic), toListAndPrint("basicUnfolded", basicUnfolded)));
    }

    @Test
    public void testBasicRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Vertex> basic = g.V().has("id", "l1-0")
                .repeat(__.limit(2).out("knows"))
                .times(2);
        GraphTraversal<Vertex, Vertex> basicUnfolded = g.V().has("id", "l1-0")
                .limit(2).out("knows")
                .limit(2).out("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("basic", basic), toListAndPrint("basicUnfolded", basicUnfolded)));

    }

    @Test
    public void testRepeatOutLimit() {
        GraphTraversal<Vertex, Path> outLimit = g.V().has("id", "l2-0")
                .repeat(__.out("knows").order().by("id").limit(2))
                .times(2)
                .path().by(T.id);
        GraphTraversal<Vertex, Path> outLimitUnfolded = g.V().has("id", "l2-0")
                .out("knows").order().by("id").limit(2)
                .out("knows").order().by("id").limit(2)
                .path().by(T.id);
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("outLimit", outLimit), toListAndPrint("outLimitUnfolded", outLimitUnfolded)));
    }

    @Test
    public void testRepeatWithBothAndLimit() {
        GraphTraversal<Vertex, Vertex> bothLimit = g.V().has("id", "l3-0")
                .repeat(__.both("knows").order().by("id").limit(3))
                .times(2);
        GraphTraversal<Vertex, Vertex> bothLimitUnfolded = g.V().has("id", "l3-0")
                .both("knows").order().by("id").limit(3)
                .both("knows").order().by("id").limit(3);
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("bothLimit", bothLimit), toListAndPrint("bothLimitUnfolded", bothLimitUnfolded)));
    }

    @Test
    public void testRepeatWithFilterAndLimit() {
        GraphTraversal<Vertex, Vertex> filterLimit = g.V().has("id", "l2-0")
                .repeat(__.out("knows").has("id", P.neq("l4-3")).order().limit(2))
                .times(2);
        GraphTraversal<Vertex, Vertex> filterLimitUnfolded = g.V().has("id", "l2-0")
                .out("knows").has("id", P.neq("l4-3")).order().limit(2)
                .out("knows").has("id", P.neq("l4-3")).order().limit(2);
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("filterLimit", filterLimit), toListAndPrint("filterLimitUnfolded", filterLimitUnfolded)));
    }

    @Test
    public void testChainedRepeatLimit() {
        GraphTraversal<Vertex, Vertex> chained = g.V().has("id", "l2-0")
                .repeat(__.order().by("id").limit(1).out("knows")).times(2)
                .repeat(__.order().by("id").limit(1).in("knows")).times(2);
        GraphTraversal<Vertex, Vertex> chainedUnfolded = g.V().has("id", "l2-0")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("chained", chained),  toListAndPrint("chainedUnfolded", chainedUnfolded)));
    }

    @Test
    public void testChainedRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Vertex> chained = g.V().has("id", "l2-0")
                .repeat(__.order().by("id").limit(2).out("knows")).times(2)
                .repeat(__.order().by("id").limit(3).in("knows")).times(2);
        GraphTraversal<Vertex, Vertex> chainedUnfolded = g.V().has("id", "l2-0")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(3).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("chained", chained),  toListAndPrint("chainedUnfolded", chainedUnfolded)));
    }

    @Test
    public void testNestedRepeatLimit() {
        GraphTraversal<Vertex, Vertex> nested = g.V().has("id", "l3-0")
                .repeat(__.order().by("id").limit(1).out("knows")
                        .repeat(__.order().by("id").limit(1).in("knows"))
                        .times(2))
                .times(2);
        GraphTraversal<Vertex, Vertex> nestedUnfolded = g.V().has("id", "l3-0")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("nested", nested), toListAndPrint("nestedUnfolded", nestedUnfolded)));

        GraphTraversal<Vertex, Vertex> nested2 = g.V().has("id", "l3-0").out("knows").order().by("id").
                repeat(__.order().by("id").limit(1).out("knows")
                        .repeat(__.order().by("id").limit(1).in("knows"))
                        .times(2)).
                times(2);
        GraphTraversal<Vertex, Vertex> nested2Unfolded = g.V().has("id", "l3-0").out("knows").order().by("id")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("nested2", nested2), toListAndPrint("nested2Unfolded", nested2Unfolded)));
    }

    @Test
    public void testNestedRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Vertex> nested = g.V().has("id", "l3-0")
                .repeat(__.order().by("id").limit(2).out("knows")
                        .repeat(__.order().by("id").limit(3).in("knows"))
                        .times(2))
                .times(2);
        GraphTraversal<Vertex, Vertex> nestedUnfolded = g.V().has("id", "l3-0")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(2).out("knows")
                .order().by("id").limit(3).in("knows")
                .order().by("id").limit(3).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("nested", nested), toListAndPrint("nestedUnfolded", nestedUnfolded)));

        GraphTraversal<Vertex, Vertex> nested2 = g.V().has("id", "l3-0").out("knows").
                repeat(__.order().by("id").limit(1).out("knows")
                        .repeat(__.order().by("id").limit(1).in("knows"))
                        .times(2)).
                times(2);
        GraphTraversal<Vertex, Vertex> nested2Unfolded = g.V().has("id", "l3-0").out("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).out("knows")
                .order().by("id").limit(1).in("knows")
                .order().by("id").limit(1).in("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("nested2", nested2), toListAndPrint("nested2Unfolded", nested2Unfolded)));
    }

    @Test
    public void testTripleNestedRepeatLimit() {
        GraphTraversal<Vertex, Vertex> tripleNested = g.V().has("id", "l1-0")
                .repeat(__.limit(1).out("knows")
                        .repeat(__.limit(1).in("knows")
                                .repeat(__.limit(1).out("knows"))
                                .times(2))
                        .times(2))
                .times(2);
        GraphTraversal<Vertex, Vertex> tripleNestedUnfolded = g.V().has("id", "l1-0")
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
                .limit(1).out("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("tripleNested", tripleNested), toListAndPrint("tripleNestedUnfolded", tripleNestedUnfolded)));
    }

    @Test
    public void testTripleNestedRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Vertex> tripleNested = g.V().has("id", "l1-0")
                .repeat(__.limit(2).out("knows")
                        .repeat(__.limit(3).in("knows")
                                .repeat(__.limit(4).out("knows"))
                                .times(2))
                        .times(2))
                .times(2);
        GraphTraversal<Vertex, Vertex> tripleNestedUnfolded = g.V().has("id", "l1-0")
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
                .limit(4).out("knows");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("tripleNested", tripleNested), toListAndPrint("tripleNestedUnfolded", tripleNestedUnfolded)));
    }

    @Test
    public void testAggregateRepeatLimit() {
        GraphTraversal<Vertex, Object> aggregate = g.V().has("id", "l1-0")
                .repeat(__.limit(1).out("knows").aggregate("x"))
                .times(2)
                .cap("x");
        GraphTraversal<Vertex, Object> aggregateUnfolded = g.V().has("id", "l1-0")
                .limit(1).out("knows").aggregate("x")
                .limit(1).out("knows").aggregate("x")
                .cap("x");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("aggregate", aggregate), toListAndPrint("aggregateUnfolded", aggregateUnfolded)));
    }

    @Test
    public void testAggregateRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Object> aggregate = g.V().has("id", "l1-0")
                .repeat(__.limit(3).out("knows").aggregate("x"))
                .times(2)
                .cap("x");
        GraphTraversal<Vertex, Object> aggregateUnfolded = g.V().has("id", "l1-0")
                .limit(3).out("knows").aggregate("x")
                .limit(3).out("knows").aggregate("x")
                .cap("x");
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("aggregate", aggregate), toListAndPrint("aggregateUnfolded", aggregateUnfolded)));
    }

    @Test
    public void testUnionRepeatLimit() {
        GraphTraversal<Vertex, Vertex> union = g.V().has("id", "l1-0")
                .union(out().limit(1), out().out().limit(1))
                .repeat(__.limit(1)).times(1);
        GraphTraversal<Vertex, Vertex> unionUnfolded = g.V().has("id", "l1-0")
                .union(out().limit(1), out().out().limit(1))
                .limit(1);
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("union", union), toListAndPrint("unionUnfolded", unionUnfolded)));
    }

    @Test
    public void testUnionRepeatLimitIncreasedLimit() {
        GraphTraversal<Vertex, Vertex> union = g.V().has("id", "l1-0")
                .union(out().limit(1), out().out().limit(1))
                .repeat(__.limit(3)).times(1);
        GraphTraversal<Vertex, Vertex> unionUnfolded = g.V().has("id", "l1-0")
                .union(out().limit(1), out().out().limit(1))
                .limit(3);
        assertTrue(CollectionUtils.isEqualCollection(toListAndPrint("union", union), toListAndPrint("unionUnfolded", unionUnfolded)));
    }
    
    private List toListAndPrint(String header, GraphTraversal t) {
        System.out.println("=====" + header + "===================================");
        System.out.println(t);
        List<?> list = t.toList();
        for (Object o : list) {
            System.out.println(o);
        }
        assertFalse(list.isEmpty());
        return list;
    }
    
    private static void load(GraphTraversalSource g) {
        g.V().drop();

        g.addV("node").property("id","l1-0").iterate();

        g.addV("node").property("id","l2-0").iterate();
        g.addV("node").property("id","l2-1").iterate();

        g.addV("node").property("id","l3-0").iterate();
        g.addV("node").property("id","l3-1").iterate();
        g.addV("node").property("id","l3-2").iterate();

        g.addV("node").property("id","l4-0").iterate();
        g.addV("node").property("id","l4-1").iterate();
        g.addV("node").property("id","l4-2").iterate();
        g.addV("node").property("id","l4-3").iterate();

        g.addV("node").property("id","l5-0").iterate();
        g.addV("node").property("id","l5-1").iterate();
        g.addV("node").property("id","l5-2").iterate();
        g.addV("node").property("id","l5-3").iterate();
        g.addV("node").property("id","l5-4").iterate();

        g.V().has("id","l1-0").as("n1").V().has("id","l2-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l1-0").as("n1").V().has("id","l2-1").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l2-0").as("n1").V().has("id","l3-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l2-0").as("n1").V().has("id","l3-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l2-0").as("n1").V().has("id","l3-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-2").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l3-0").as("n1").V().has("id","l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-3").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l4-0").as("n1").V().has("id","l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-4").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l1-0").as("n1").V().has("id","l2-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l1-0").as("n1").V().has("id","l2-1").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l2-0").as("n1").V().has("id","l3-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l2-0").as("n1").V().has("id","l3-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l2-0").as("n1").V().has("id","l3-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l2-1").as("n1").V().has("id","l3-2").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l3-0").as("n1").V().has("id","l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-0").as("n1").V().has("id","l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-1").as("n1").V().has("id","l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l3-2").as("n1").V().has("id","l4-3").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l4-0").as("n1").V().has("id","l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-0").as("n1").V().has("id","l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-1").as("n1").V().has("id","l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-2").as("n1").V().has("id","l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l4-3").as("n1").V().has("id","l5-4").as("n2").addE("friend").from("n1").to("n2").iterate();

        // Add layer 6 nodes
        g.addV("node").property("id","l6-0").iterate();
        g.addV("node").property("id","l6-1").iterate();
        g.addV("node").property("id","l6-2").iterate();
        g.addV("node").property("id","l6-3").iterate();
        g.addV("node").property("id","l6-4").iterate();
        g.addV("node").property("id","l6-5").iterate();
        
        g.V().has("id","l5-0").as("n1").V().has("id","l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l5-1").as("n1").V().has("id","l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l5-2").as("n1").V().has("id","l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l5-3").as("n1").V().has("id","l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l5-4").as("n1").V().has("id","l6-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-5").as("n2").addE("knows").from("n1").to("n2").iterate();

        // Connect layer 5 to layer 6 with "friend" edges (same pattern)
        g.V().has("id","l5-0").as("n1").V().has("id","l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-0").as("n1").V().has("id","l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l5-1").as("n1").V().has("id","l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-1").as("n1").V().has("id","l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l5-2").as("n1").V().has("id","l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-2").as("n1").V().has("id","l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l5-3").as("n1").V().has("id","l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-3").as("n1").V().has("id","l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l5-4").as("n1").V().has("id","l6-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l5-4").as("n1").V().has("id","l6-5").as("n2").addE("friend").from("n1").to("n2").iterate();

        // Add layer 7 nodes
        g.addV("node").property("id","l7-0").iterate();
        g.addV("node").property("id","l7-1").iterate();
        g.addV("node").property("id","l7-2").iterate();
        g.addV("node").property("id","l7-3").iterate();
        g.addV("node").property("id","l7-4").iterate();
        g.addV("node").property("id","l7-5").iterate();
        g.addV("node").property("id","l7-6").iterate();

        // Connect layer 6 to layer 7 with "knows" edges
        g.V().has("id","l6-0").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l6-1").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l6-2").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l6-3").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l6-4").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        g.V().has("id","l6-5").as("n1").V().has("id","l7-0").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-1").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-2").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-3").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-4").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-5").as("n2").addE("knows").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-6").as("n2").addE("knows").from("n1").to("n2").iterate();

        // Connect layer 6 to layer 7 with "friend" edges
        g.V().has("id","l6-0").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-0").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l6-1").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-1").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l6-2").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-2").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l6-3").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-3").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l6-4").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-4").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();

        g.V().has("id","l6-5").as("n1").V().has("id","l7-0").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-1").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-2").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-3").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-4").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-5").as("n2").addE("friend").from("n1").to("n2").iterate();
        g.V().has("id","l6-5").as("n1").V().has("id","l7-6").as("n2").addE("friend").from("n1").to("n2").iterate();
    }
    
    @Test
    @Ignore
    public void testPlay8() throws Exception {
        Graph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        System.out.println(g.withSack(1).inject(1).repeat(sack((BiFunction)sum).by(constant(1))).times(10).emit().math("sin _").by(sack()).toList());
    }

    @Test
    @Ignore
    public void benchmarkStandardTraversals() throws Exception {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();
        graph.io(GraphMLIo.build()).readGraph("data/grateful-dead.xml");
        final List<Supplier<Traversal>> traversals = Arrays.asList(
                () -> g.V().outE().inV().outE().inV().outE().inV(),
                () -> g.V().out().out().out(),
                () -> g.V().out().out().out().path(),
                () -> g.V().repeat(out()).times(2),
                () -> g.V().repeat(out()).times(3),
                () -> g.V().local(out().out().values("name").fold()),
                () -> g.V().out().local(out().out().values("name").fold()),
                () -> g.V().out().map(v -> g.V(v.get()).out().out().values("name").toList())
        );
        traversals.forEach(traversal -> {
            logger.info("\nTESTING: {}", traversal.get());
            for (int i = 0; i < 7; i++) {
                final long t = System.currentTimeMillis();
                traversal.get().iterate();
                System.out.print("   " + (System.currentTimeMillis() - t));
            }
        });
    }

    @Test
    @Ignore
    public void testPlay4() throws Exception {
        Graph graph = TinkerGraph.open();
        graph.io(GraphMLIo.build()).readGraph("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");
        GraphTraversalSource g = graph.traversal();
        final List<Supplier<Traversal>> traversals = Arrays.asList(
                () -> g.V().has(T.label, "song").out().groupCount().<Vertex>by(t ->
                        g.V(t).choose(r -> g.V(r).has(T.label, "artist").hasNext(),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name").next()).fold(),
                () -> g.V().has(T.label, "song").out().groupCount().<Vertex>by(t ->
                        g.V(t).choose(has(T.label, "artist"),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name").next()).fold(),
                () -> g.V().has(T.label, "song").out().groupCount().by(
                        choose(has(T.label, "artist"),
                                in("writtenBy", "sungBy"),
                                both("followedBy")).values("name")).fold(),
                () -> g.V().has(T.label, "song").both().groupCount().<Vertex>by(t -> g.V(t).both().values("name").next()),
                () -> g.V().has(T.label, "song").both().groupCount().by(both().values("name")));
        traversals.forEach(traversal -> {
            logger.info("\nTESTING: {}", traversal.get());
            for (int i = 0; i < 10; i++) {
                final long t = System.currentTimeMillis();
                traversal.get().iterate();
                //System.out.println(traversal.get().toList());
                System.out.print("   " + (System.currentTimeMillis() - t));
            }
        });
    }

    @Test
    @Ignore
    public void testPlayDK() throws Exception {
        final Graph graph = TinkerGraph.open();
        final EventStrategy strategy = EventStrategy.build().addListener(new ConsoleMutationListener(graph)).create();
        final GraphTraversalSource g = graph.traversal().withStrategies(strategy);

        g.addV().property(T.id, 1).iterate();
        g.V(1).property("name", "name1").iterate();
        g.V(1).property("name", "name2").iterate();
        g.V(1).property("name", "name2").iterate();

        g.addV().property(T.id, 2).iterate();
        g.V(2).property(VertexProperty.Cardinality.list, "name", "name1").iterate();
        g.V(2).property(VertexProperty.Cardinality.list, "name", "name2").iterate();
        g.V(2).property(VertexProperty.Cardinality.list, "name", "name2").iterate();


        g.addV().property(T.id, 3).iterate();
        g.V(3).property(VertexProperty.Cardinality.set, "name", "name1", "ping", "pong").iterate();
        g.V(3).property(VertexProperty.Cardinality.set, "name", "name2", "ping", "pong").iterate();
        g.V(3).property(VertexProperty.Cardinality.set, "name", "name2", "pong", "ping").iterate();
    }

    @Test
    @Ignore
    public void testPlay7() throws Exception {
        /*TinkerGraph graph = TinkerGraph.open();
        graph.createIndex("name",Vertex.class);
        graph.io(GraphMLIo.build()).readGraph("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");*/
        //System.out.println(g.V().properties().key().groupCount().next());
        TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        final List<Supplier<GraphTraversal<?, ?>>> traversals = Arrays.asList(
                () -> g.V().out().as("v").match(
                        as("v").outE().count().as("outDegree"),
                        as("v").inE().count().as("inDegree")).select("v", "outDegree", "inDegree").by(valueMap()).by().by().local(union(select("v"), select("inDegree", "outDegree")).unfold().fold())
        );

        traversals.forEach(traversal -> {
            logger.info("pre-strategy:  {}", traversal.get());
            logger.info("post-strategy: {}", traversal.get().iterate());
            logger.info(TimeUtil.clockWithResult(50, () -> traversal.get().toList()).toString());
        });
    }

    @Test
    @Ignore
    public void testPlay5() throws Exception {

        TinkerGraph graph = TinkerGraph.open();
        graph.createIndex("name", Vertex.class);
        graph.io(GraphMLIo.build()).readGraph("/Users/marko/software/tinkerpop/tinkerpop3/data/grateful-dead.xml");
        GraphTraversalSource g = graph.traversal();

        final Supplier<Traversal<?, ?>> traversal = () ->
                g.V().match(
                        as("a").has("name", "Garcia"),
                        as("a").in("writtenBy").as("b"),
                        as("b").out("followedBy").as("c"),
                        as("c").out("writtenBy").as("d"),
                        as("d").where(neq("a"))).select("a", "b", "c", "d").by("name");


        logger.info(traversal.get().toString());
        logger.info(traversal.get().iterate().toString());
        traversal.get().forEachRemaining(x -> logger.info(x.toString()));

    }

    @Test
    @Ignore
    public void testPaths() throws Exception {
        TinkerGraph graph = TinkerGraph.open();
        graph.io(GraphMLIo.build()).readGraph("/Users/twilmes/work/repos/scratch/tinkerpop/gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/graphml/grateful-dead.xml");
//        graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal().withComputer(Computer.compute().workers(1));

        System.out.println(g.V().match(
                as("a").in("sungBy").as("b"),
                as("a").in("sungBy").as("c"),
                as("b").out("writtenBy").as("d"),
                as("c").out("writtenBy").as("e"),
                as("d").has("name", "George_Harrison"),
                as("e").has("name", "Bob_Marley")).select("a").count().next());

//        System.out.println(g.V().out("created").
//                project("a","b").
//                by("name").
//                by(__.in("created").count()).
//                order().by(select("b")).
//                select("a").toList());

//        System.out.println(g.V().as("a").out().where(neq("a")).barrier().out().count().profile().next());
//        System.out.println(g.V().out().as("a").where(out().select("a").values("prop").count().is(gte(1))).out().where(neq("a")).toList());
//        System.out.println(g.V().match(
//                __.as("a").out().as("b"),
//                __.as("b").out().as("c")).select("c").count().profile().next());

    }

    @Test
    @Ignore
    public void testPlay9() throws Exception {
        Graph graph = TinkerGraph.open();
        graph.io(GraphMLIo.build()).readGraph("../data/grateful-dead.xml");

        GraphTraversalSource g = graph.traversal().withComputer(Computer.compute().workers(4)).withStrategies(PathRetractionStrategy.instance());
        GraphTraversalSource h = graph.traversal().withComputer(Computer.compute().workers(4)).withoutStrategies(PathRetractionStrategy.class);

        for (final GraphTraversalSource source : Arrays.asList(g, h)) {
            System.out.println(source.V().match(
                    as("a").in("sungBy").as("b"),
                    as("a").in("sungBy").as("c"),
                    as("b").out("writtenBy").as("d"),
                    as("c").out("writtenBy").as("e"),
                    as("d").has("name", "George_Harrison"),
                    as("e").has("name", "Bob_Marley")).select("a").count().profile().next());
        }
    }

    @Test
    @Ignore
    public void testPlay6() throws Exception {
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        for (int i = 0; i < 1000; i++) {
            graph.addVertex(T.label, "person", T.id, i);
        }
        graph.vertices().forEachRemaining(a -> {
            graph.vertices().forEachRemaining(b -> {
                if (a != b) {
                    a.addEdge("knows", b);
                }
            });
        });
        graph.vertices(50).next().addEdge("uncle", graph.vertices(70).next());
        logger.info(TimeUtil.clockWithResult(500, () -> g.V().match(as("a").out("knows").as("b"), as("a").out("uncle").as("b")).toList()).toString());
    }

    @Test
    @Ignore
    public void testBugs() {
        final GraphTraversalSource g = TinkerFactory.createModern().traversal();
        Object o1 = g.V().map(__.V(1));
        System.out.println(g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b").explain());
        System.out.println(g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b").toList());

        Traversal<?,?> t =
                g.V("3").
                        union(__.repeat(out().simplePath()).times(2).count(),
                                __.repeat(in().simplePath()).times(2).count());
    }

    @Test
    @Ignore
    public void WithinTest() {
        final int count = 500;

        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        for (int i = 0; i < count; i++) {
            graph.addVertex(T.label, "person", T.id, i);
        }

        graph.vertices().forEachRemaining(a -> {
            graph.vertices().forEachRemaining(b -> {
                if (a != b && ((int) a.id() < 2 || (int) b.id() < 2)) {
                    a.addEdge("knows", b);
                }
            });
        });

        final long start = System.currentTimeMillis();

        final List result = g.
                V().has(T.id,P.lt(count/2)).aggregate("v1").
                V().where(P.without("v1")).
                toList();

        System.out.println(result.size());
        System.out.printf("Done in %10d %n", System.currentTimeMillis() - start);
    }
}
