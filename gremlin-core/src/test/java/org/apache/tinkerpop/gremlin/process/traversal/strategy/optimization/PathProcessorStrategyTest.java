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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class PathProcessorStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final Traversal.Admin<?, ?> rootTraversal = new DefaultGraphTraversal<>();
        final TraversalParent parent = new TraversalVertexProgramStep(rootTraversal, this.original.asAdmin());
        rootTraversal.addStep(parent.asStep());
        this.original.asAdmin().setParent(parent); // trick it into OLAP
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(PathProcessorStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(this.optimized, this.original);
    }


    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Object[][]{
                // select("a")
                {__.select("a"), __.select("a"), Collections.emptyList()},
                {__.select("a").by(), __.select("a").by(), Collections.emptyList()},
                {__.select("a").by(__.outE().count()), __.select("a").map(__.outE().count()), Collections.emptyList()},
                {__.select("a").by("name"), __.select("a").map(new ElementValueTraversal<>("name")), Collections.emptyList()},
                {__.select("a").out(), __.select("a").out(), Collections.emptyList()},
                {__.select(Pop.all, "a").by(__.values("name")), __.select(Pop.all, "a").by("name"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select(Pop.last, "a").by(__.values("name")), __.select(Pop.last, "a").map(__.values("name")), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select(Pop.first, "a").by(__.values("name")), __.select(Pop.first, "a").map(__.values("name")), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                // select("a","b")
                {__.select("a", "b"), __.select("a", "b"), Collections.emptyList()},
                {__.select("a", "b").by(), __.select("a", "b").by(), Collections.emptyList()},
                {__.select("a", "b", "c").by(), __.select("a", "b", "c").by(), Collections.emptyList()},
                {__.select("a", "b").by().by("age"), __.select("b").map(new ElementValueTraversal<>("age")).as("b").select("a").map(new IdentityTraversal<>()).as("a").select(Pop.last, "a", "b"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select("a", "b").by("name").by("age"), __.select("b").map(new ElementValueTraversal<>("age")).as("b").select("a").map(new ElementValueTraversal<>("name")).as("a").select(Pop.last, "a", "b"), Collections.emptyList()},
                {__.select("a", "b", "c").by("name").by(__.outE().count()), __.select("c").map(new ElementValueTraversal<>("name")).as("c").select("b").map(__.outE().count()).as("b").select("a").map(new ElementValueTraversal<>("name")).as("a").select(Pop.last, "a", "b", "c"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select(Pop.first, "a", "b").by("name").by("age"), __.select(Pop.first, "b").map(new ElementValueTraversal<>("age")).as("b").select(Pop.first, "a").map(new ElementValueTraversal<>("name")).as("a").select(Pop.last, "a", "b"), Collections.emptyList()},
                {__.select(Pop.last, "a", "b").by("name").by("age"), __.select(Pop.last, "b").map(new ElementValueTraversal<>("age")).as("b").select(Pop.last, "a").map(new ElementValueTraversal<>("name")).as("a").select(Pop.last, "a", "b"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select(Pop.all, "a", "b").by("name").by("age"), __.select(Pop.all, "a", "b").by("name").by("age"), Collections.emptyList()},
                {__.select(Pop.mixed, "a", "b").by("name").by("age"), __.select(Pop.mixed, "a", "b").by("name").by("age"), Collections.emptyList()},
                // where(as("a")...)
                {__.where(__.out("knows")), __.where(__.outE("knows")), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.where(__.as("a").out("knows")), __.identity().as("xyz").select(Pop.last, "a").filter(__.out("knows")).select(Pop.last, "xyz"), Collections.emptyList()},
                {__.where(__.as("a").has("age", P.gt(10))), __.identity().as("xyz").select(Pop.last, "a").has("age", P.gt(10)).select(Pop.last, "xyz"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.select("b").where(__.as("a").has("age", P.gt(10))), __.select("b").as("xyz").select(Pop.last, "a").has("age", P.gt(10)).select(Pop.last, "xyz"), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.where(__.as("a").out("knows").as("b")), __.identity().as("xyz").select(Pop.last, "a").where(__.out("knows").as("b")).select(Pop.last, "xyz"), Collections.emptyList()},
                {__.where("a", P.eq("b")), __.where("a", P.eq("b")), Collections.emptyList()},
                {__.as("a").out().where(__.as("a").has("age", P.gt(10))).in(), __.as("a").out().as("xyz").select(Pop.last, "a").has("age", P.gt(10)).select(Pop.last, "xyz").in(), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
                {__.as("a").out().where(__.as("a").has("age", P.gt(10))).in().path(), __.as("a").out().where(__.as("a").has("age", P.gt(10))).in().path(), TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList()},
        });
    }
}
