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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.path;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class RepeatUnrollStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(RepeatUnrollStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final int maxBarrierSize = RepeatUnrollStrategy.MAX_BARRIER_SIZE;
        final Predicate<Traverser<Vertex>> predicate = t -> t.loops() > 5;
        return Arrays.asList(new Object[][]{
                {__.repeat(out()).times(0), __.repeat(out()).times(0), Collections.emptyList()},
                {__.<Vertex>times(0).repeat(out()), __.<Vertex>times(0).repeat(out()), Collections.emptyList()},
                {__.identity(), __.identity(), Collections.emptyList()},
                {out().as("a").in().repeat(__.outE("created").bothV()).times(2).in(), out().as("a").in().outE("created").bothV().barrier(maxBarrierSize).outE("created").bothV().barrier(maxBarrierSize).in(), Collections.emptyList()},
                {out().repeat(__.outE("created").bothV()).times(1).in(), out().outE("created").bothV().barrier(maxBarrierSize).in(), Collections.emptyList()},
                {__.repeat(__.outE("created").bothV()).times(1).in(), __.outE("created").bothV().barrier(maxBarrierSize).in(), Collections.emptyList()},
                {__.repeat(out()).times(2).as("x").repeat(__.in().as("b")).times(3), out().barrier(maxBarrierSize).out().barrier(maxBarrierSize).as("x").in().as("b").barrier(maxBarrierSize).in().as("b").barrier(maxBarrierSize).in().as("b").barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(__.outE("created").inV()).times(2), __.outE("created").inV().barrier(maxBarrierSize).outE("created").inV().barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(out()).times(3), out().barrier(maxBarrierSize).out().barrier(maxBarrierSize).out().barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(__.local(__.select("a").out("knows"))).times(2), __.local(__.select("a").out("knows")).barrier(maxBarrierSize).local(__.select("a").out("knows")).barrier(maxBarrierSize), Collections.emptyList()},
                {__.<Vertex>times(2).repeat(out()), out().barrier(maxBarrierSize).out().barrier(maxBarrierSize), Collections.emptyList()},
                {__.<Vertex>out().times(2).repeat(out().as("a")).as("x"), out().out().as("a").barrier(maxBarrierSize).out().as("a").barrier(maxBarrierSize).as("x"), Collections.emptyList()},
                {__.repeat(out()).emit().times(2), __.repeat(out()).emit().times(2), Collections.emptyList()},
                {__.repeat(out()).until(predicate), __.repeat(out()).until(predicate), Collections.emptyList()},
                {__.repeat(out()).until(predicate).repeat(out()).times(2), __.repeat(out()).until(predicate).out().barrier(maxBarrierSize).out().barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(__.union(__.both(), __.identity())).times(2).out(), __.union(__.both(), __.identity()).barrier(maxBarrierSize).union(__.both(), __.identity()).barrier(maxBarrierSize).out(), Collections.emptyList()},
                {__.in().repeat(out("knows")).times(3).as("a").count().is(0), __.in().out("knows").barrier(maxBarrierSize).out("knows").barrier(maxBarrierSize).out("knows").as("a").count().is(0), Collections.emptyList()},
                //
                {__.repeat(__.outE().inV()).times(2), __.outE().inV().barrier(maxBarrierSize).outE().inV().barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(__.outE().filter(path()).inV()).times(2), __.outE().filter(path()).inV().barrier(maxBarrierSize).outE().filter(path()).inV().barrier(maxBarrierSize), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                {__.repeat(__.outE().inV()).times(2), __.out().barrier(maxBarrierSize).out().barrier(maxBarrierSize), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                // Nested Loop tests
                {__.repeat(out().repeat(out()).times(0)).times(1), __.out().repeat(out()).times(0).barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(out().repeat(out()).times(1)).times(1), __.out().out().barrier(maxBarrierSize), Collections.emptyList()},
                {__.repeat(out()).until(__.repeat(out()).until(predicate)), __.repeat(out()).until(__.repeat(out()).until(predicate)), Collections.emptyList()},
                {__.repeat(__.repeat(out()).times(2)).until(predicate), __.repeat(__.out().barrier(maxBarrierSize).out().barrier(maxBarrierSize)).until(predicate), Collections.emptyList()},
                {__.repeat(__.repeat(out("created")).until(__.has("name", "ripple"))), __.repeat(__.repeat(out("created")).until(__.has("name", "ripple"))), Collections.emptyList()},

        });
    }
}
