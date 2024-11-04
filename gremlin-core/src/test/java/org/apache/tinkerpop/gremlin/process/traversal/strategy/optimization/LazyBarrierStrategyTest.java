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
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(LazyBarrierStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(repr, this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        final int LAZY_SIZE = LazyBarrierStrategy.MAX_BARRIER_SIZE;
        final int REPEAT_SIZE = RepeatUnrollStrategy.MAX_BARRIER_SIZE;
        final int PATH_SIZE = PathRetractionStrategy.MAX_BARRIER_SIZE;
        return Arrays.asList(new Object[][]{
                {__.out().count(), __.out().count(), Collections.emptyList()},
                {__.out().out().count(), __.out().out().count(), Collections.emptyList()},
                {__.out().out().out().count(), __.out().out().barrier(LAZY_SIZE).out().count(), Collections.emptyList()},
                {__.out().out().out().out().count(), __.out().out().barrier(LAZY_SIZE).out().barrier(LAZY_SIZE).out().count(), Collections.emptyList()},
                {__.out().out().out().count(), __.out().out().barrier(LAZY_SIZE).outE().count(), Arrays.asList(CountStrategy.instance(), AdjacentToIncidentStrategy.instance())},
                {__.out().out().out().count().is(P.gt(10)), __.out().out().barrier(LAZY_SIZE).outE().limit(11).count().is(P.gt(10)), Arrays.asList(CountStrategy.instance(), AdjacentToIncidentStrategy.instance())},
                {__.outE().inV().outE().inV().outE().inV().groupCount(), __.outE().inV().outE().inV().barrier(LAZY_SIZE).outE().inV().groupCount(), Collections.emptyList()},
                {__.outE().inV().outE().inV().outE().inV().groupCount(), __.out().out().barrier(LAZY_SIZE).out().groupCount(), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                {__.out().out().has("age", 32).out().count(), __.out().out().barrier(LAZY_SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().out().has("age", 32).out().count(), __.V().out().barrier(LAZY_SIZE).out().barrier(LAZY_SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().has("age", 32).out().count(), __.V().out().barrier(LAZY_SIZE).has("age", 32).out().count(), Collections.emptyList()},
                {__.V().out().has("age", 32).V().out().count(), __.V().out().barrier(LAZY_SIZE).has("age", 32).V().barrier(LAZY_SIZE).out().count(), Collections.emptyList()},
                {__.repeat(__.out()).times(4), __.repeat(__.out()).times(4), Collections.emptyList()},
                {__.repeat(__.out()).times(4), __.out().barrier(REPEAT_SIZE).out().barrier(REPEAT_SIZE).out().barrier(REPEAT_SIZE).out().barrier(REPEAT_SIZE), Collections.singletonList(RepeatUnrollStrategy.instance())},
                {__.out().out().as("a").select("a").out(), __.out().out().barrier(LAZY_SIZE).as("a").select("a").out(), Collections.emptyList()},
                {__.out().out().as("a").select("a").out(), __.out().out().barrier(LAZY_SIZE).as("a").select("a").barrier(PATH_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().out().as("a").out().select("a").out(), __.out().out().barrier(LAZY_SIZE).as("a").out().select("a").barrier(PATH_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().out().out().limit(10).out(), __.out().out().barrier(LAZY_SIZE).out().limit(10).out(), Collections.emptyList()},
                {__.V().out().in().where(P.neq("a")), __.V().out().barrier(LAZY_SIZE).in().barrier(LAZY_SIZE).where(P.neq("a")), Collections.emptyList()},
                {__.V().as("a").out().in().where(P.neq("a")), __.V().as("a").out().in().where(P.neq("a")), Collections.emptyList()},
                {__.out().out().in().where(P.neq("a")), __.out().out().barrier(LAZY_SIZE).in().barrier(LAZY_SIZE).where(P.neq("a")), Collections.emptyList()},
                {__.out().as("a").out().in().where(P.neq("a")), __.out().as("a").out().in().where(P.neq("a")), Collections.emptyList()},
                {__.out().as("a").out().in().where(P.neq("a")).out().out(), __.out().as("a").out().in().where(P.neq("a")).barrier(LAZY_SIZE).out().barrier(LAZY_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(PATH_SIZE).out().barrier(LAZY_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).out().out(), Collections.emptyList()},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().select("b").out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(PATH_SIZE).out().select("b").barrier(PATH_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.out().as("a").out().as("b").in().where(P.neq("a")).out().select("b").out().out(), __.out().as("a").out().as("b").in().where(P.neq("a")).barrier(PATH_SIZE).out().select("b").barrier(PATH_SIZE).out().barrier(LAZY_SIZE).out(), Collections.singletonList(PathRetractionStrategy.instance())},
                {__.V().out().out().groupCount().by(__.out().out().out()).out(), __.V().out().barrier(LAZY_SIZE).out().groupCount().by(__.out().out().barrier(LAZY_SIZE).out()).out(), Collections.emptyList()},
                {__.V().out().out().groupCount().by(__.out().out().out()).out().as("a"), __.V().out().barrier(LAZY_SIZE).out().groupCount().by(__.out().out().barrier(LAZY_SIZE).out()).out().as("a"), Collections.emptyList()},
                {__.out().drop(), __.out().drop(), Collections.emptyList()},
                {__.out().properties().drop(), __.out().properties().drop(), Collections.emptyList()},
                {__.out().properties().properties().drop(), __.out().properties().properties().drop(), Collections.emptyList()},
                {__.out().out().properties().drop(), __.out().out().properties().drop(), Collections.emptyList()},
                {__.out().out().values().is(true), __.out().out().barrier(LAZY_SIZE).values().barrier(LAZY_SIZE).is(true), Collections.emptyList()},
                {__.outE().drop(), __.outE().drop(), Collections.emptyList()},
                {__.outE().properties().drop(), __.outE().properties().drop(), Collections.emptyList()},
                {__.V().out().out().groupCount().by(__.out().out().out()).out().as("a"), __.V().out().barrier(LAZY_SIZE).out().groupCount().by(__.out().out().barrier(LAZY_SIZE).out()).out().as("a"), Collections.emptyList()},
                {__.V().both().profile(), __.V().both().profile(), Collections.emptyList() },
                {__.V().both().both().profile(), __.V().both().barrier(LAZY_SIZE).both().profile(), Collections.emptyList() },
                {__.V().both().local(__.both().both().out()).profile(), __.V().both().barrier(LAZY_SIZE).local(__.both().both().barrier(LAZY_SIZE).out()).profile(), Collections.emptyList() },
                {__.V().both().local(__.both().both().out()).in().profile(), __.V().both().barrier(LAZY_SIZE).local(__.both().both().barrier(LAZY_SIZE).out()).in().profile(), Collections.emptyList() }
        });
    }
}
