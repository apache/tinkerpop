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

package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class SingleIterationStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final Traversal.Admin<?, ?> rootTraversal = new DefaultGraphTraversal<>();
        final TraversalVertexProgramStep parent = new TraversalVertexProgramStep(rootTraversal, this.original.asAdmin());
        rootTraversal.addStep(parent.asStep());
        parent.setComputerTraversal(this.original.asAdmin());
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(SingleIterationStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        rootTraversal.setStrategies(strategies);
        rootTraversal.asAdmin().applyStrategies();
        assertEquals(this.optimized, parent.computerTraversal.get());
    }


    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Object[][]{
                {__.V().out().count(), __.V().outE().count(), Collections.singletonList(AdjacentToIncidentStrategy.instance())},
                {__.V().id(), __.V().id(), Collections.emptyList()},
                {__.V().out().count(), __.V().out().count(), Collections.emptyList()},
                {__.V().out().label().count(), __.V().out().label().count(), Collections.emptyList()},
                {__.V().in().id(), __.V().local(in().id()), Collections.emptyList()},
                {in().id(), in().id(), Collections.emptyList()}, // test inject
                {__.V().out().id(), __.V().local(out().id()), Collections.emptyList()},
                {__.V().both().id(), __.V().local(__.both().id()), Collections.emptyList()},
                {__.V().outE().inV().id().count(), __.V().local(__.outE().inV().id()).count(), Collections.emptyList()},
                {__.V().map(__.outE().inV()).count(), __.V().map(__.outE().inV()).count(), Collections.emptyList()},
                {__.V().out().map(__.outE().inV()).count(), __.V().out().map(__.outE().inV()).count(), Collections.emptyList()},
                {__.V().outE().map(__.inV()).id().count(), __.V().outE().map(__.inV()).id().count(), Collections.emptyList()},
                {__.V().outE().map(__.inV()).count(), __.V().outE().map(__.inV()).count(), Collections.emptyList()},
                {__.V().outE().map(__.inV()).values("name").count(), __.V().outE().map(__.inV()).values("name").count(), Collections.emptyList()},
                {__.V().outE().inV().count(), __.V().local(__.outE().inV()).count(), Collections.emptyList()},
                {__.V().out().id().count(), __.V().local(out().id()).count(), Collections.emptyList()},
                {__.V().in().id().count(), __.V().local(in().id()).count(), Collections.emptyList()},
                {__.V().in().id().select("id-map").dedup().count(), __.V().local(in().id().select("id-map")).dedup().count(), Collections.emptyList()},
                {__.V().in().id().groupCount().select(keys).unfold().dedup().count(), __.V().local(in().id()).groupCount().select(keys).unfold().dedup().count(), Collections.emptyList()},
                {__.V().outE().values("weight"), __.V().outE().values("weight"), Collections.emptyList()},
                {__.V().outE().values("weight").sum(), __.V().outE().values("weight").sum(), Collections.emptyList()},
                {__.V().inE().values("weight"), __.V().local(inE().values("weight")), Collections.emptyList()},
                {__.V().inE().values("weight").sum(), __.V().local(inE().values("weight")).sum(), Collections.emptyList()},
                {__.V().inE().values("weight").sum().dedup().count(), __.V().local(inE().values("weight")).sum().dedup().count(), Collections.emptyList()},
                {__.V().as("a").out("knows").as("b").select("a", "b"), __.V().as("a").out("knows").as("b").select("a", "b"), Collections.emptyList()},
                {__.V().out().groupCount("x").cap("x"), __.V().out().groupCount("x").cap("x"), Collections.emptyList()},
                {__.V().outE().inV().groupCount().select(Column.values).unfold().dedup().count(), __.V().outE().inV().groupCount().select(Column.values).unfold().dedup().count(), Collections.emptyList()},
                {__.V().outE().inV().groupCount(), __.V().outE().inV().groupCount(), Collections.emptyList()},
                {__.V().outE().inV().groupCount().by("name"), __.V().outE().inV().groupCount().by("name"), Collections.emptyList()},
                {__.V().inE().id().groupCount(), __.V().local(inE().id()).groupCount(), Collections.emptyList()},
                {__.V().inE().values("weight").groupCount(), __.V().local(inE().values("weight")).groupCount(), Collections.emptyList()},
                {__.V().outE().inV().tree(), __.V().outE().inV().tree(), Collections.emptyList()},
                {__.V().in().values("name").groupCount(), __.V().in().values("name").groupCount(), Collections.emptyList()}
        });
    }
}
