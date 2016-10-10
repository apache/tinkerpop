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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
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
public class LazyBarrierStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return generateTestParameters();
    }

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(LazyBarrierStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final int SIZE = LazyBarrierStrategy.MAX_BARRIER_SIZE;
        return Arrays.asList(new Object[][]{
                {__.out().count(), __.out().count(), Collections.emptyList()},
                {__.out().out().count(), __.out().out().count(), Collections.emptyList()},
                {__.out().out().out().count(), __.out().out().barrier(SIZE).out().count(), Collections.emptyList()},
                {__.out().out().out().out().count(), __.out().out().barrier(SIZE).out().barrier(SIZE).out().count(), Collections.emptyList()},
                {__.out().out().out().count(), __.out().out().barrier(SIZE).outE().count(), Arrays.asList(RangeByIsCountStrategy.instance(), AdjacentToIncidentStrategy.instance())},
                {__.out().out().out().count().is(P.gt(10)), __.out().out().barrier(SIZE).outE().limit(11).count().is(P.gt(10)), Arrays.asList(RangeByIsCountStrategy.instance(), AdjacentToIncidentStrategy.instance())},
                {__.outE().inV().outE().inV().outE().inV().groupCount(), __.outE().inV().outE().inV().barrier(SIZE).outE().inV().groupCount(), Collections.emptyList()},
                {__.outE().inV().outE().inV().outE().inV().groupCount(), __.out().out().barrier(SIZE).out().groupCount(), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                {__.out().out().has("age", 32).out().count(), __.out().out().barrier(SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().out().has("age", 32).out().count(), __.V().out().barrier(SIZE).out().barrier(SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().has("age", 32).out().count(), __.V().out().barrier(SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().has("age", 32).V().out().count(), __.V().out().barrier(SIZE).has("age", 32).V().barrier(SIZE).out().count(), Collections.emptyList()},
                {__.repeat(__.out()).times(4), __.repeat(__.out()).times(4), Collections.emptyList()},
                {__.repeat(__.out()).times(4), __.out().barrier(5000).out().barrier(5000).out().barrier(5000).out().barrier(5000), Collections.singletonList(RepeatUnrollStrategy.instance())},
                {__.out().out().as("a").select("a").out(), __.out().out().barrier(SIZE).as("a").select("a").out(), Collections.emptyList()},
                {__.out().out().as("a").select("a").out(), __.out().out().barrier(SIZE).as("a").select("a").barrier(2500).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().out().as("a").out().select("a").out(), __.out().out().barrier(SIZE).as("a").out().select("a").barrier(2500).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().out().out().limit(10).out(), __.out().out().barrier(SIZE).out().limit(10).out().barrier(SIZE), Collections.emptyList()},
                {__.V().out().in().where(P.neq("a")), __.V().out().barrier(SIZE).in().barrier(SIZE).where(P.neq("a")), Collections.emptyList()},
                {__.V().as("a").out().in().where(P.neq("a")), __.V().as("a").out().in().where(P.neq("a")), Collections.emptyList()},
                {__.out().out().in().where(P.neq("a")), __.out().out().barrier(SIZE).in().barrier(SIZE).where(P.neq("a")), Collections.emptyList()},
                {__.out().as("a").out().in().where(P.neq("a")), __.out().as("a").out().in().where(P.neq("a")), Collections.emptyList()},
                {__.out().as("a").out().in().where(P.neq("a")).out().out(), __.out().as("a").out().in().where(P.neq("a")).barrier(2500).out().barrier(SIZE).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(2500).out().barrier(SIZE).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), Collections.emptyList()},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().select("b").out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(2500).out().select("b").barrier(2500).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().select("b").out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(2500).out().select("b").barrier(2500).out().barrier(SIZE).out().barrier(SIZE), Collections.singletonList(PathRetractionStrategy.instance())},
        });
    }
}
