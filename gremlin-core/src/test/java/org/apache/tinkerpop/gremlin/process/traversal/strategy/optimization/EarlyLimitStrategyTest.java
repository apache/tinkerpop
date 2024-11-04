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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Parameterized.class)
public class EarlyLimitStrategyTest {

    @Parameterized.Parameter()
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy> otherStrategies;

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin();
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(EarlyLimitStrategy.instance());
        for (final TraversalStrategy strategy : this.otherStrategies) {
            strategies.addStrategies(strategy);
            if (strategy instanceof ProfileStrategy) {
                final TraversalStrategies os = new DefaultTraversalStrategies();
                os.addStrategies(ProfileStrategy.instance());
                this.optimized.asAdmin().setStrategies(os);
                this.optimized.asAdmin().applyStrategies();
            }
        }
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();
        assertEquals(repr, this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {__.out().valueMap().limit(1), __.out().limit(1).valueMap(), Collections.emptyList()},
                {__.out().limit(5).valueMap().range(5, 10), __.start().out().discard(), Collections.emptyList()},
                {__.out().limit(5).valueMap().range(6, 10), __.start().out().discard(), Collections.emptyList()},
                {__.V().out().valueMap().limit(1), __.V().out().limit(1).valueMap(), Collections.singleton(LazyBarrierStrategy.instance())},
                {__.out().out().limit(1).in().in(), __.out().out().limit(1).in().barrier(LazyBarrierStrategy.MAX_BARRIER_SIZE).in(), Collections.singleton(LazyBarrierStrategy.instance())},
                {__.out().has("name","marko").limit(1).in().in(), __.out().has("name","marko").limit(1).in().in(), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).limit(1), __.out().limit(1).map(__.identity()).map(__.identity()), Collections.singleton(LazyBarrierStrategy.instance())},
                {__.out().map(__.identity()).map(__.identity()).limit(1).as("a"), __.out().limit(1).map(__.identity()).map(__.identity()).as("a"), Collections.singleton(LazyBarrierStrategy.instance())},
                {__.out().map(__.identity()).map(__.identity()).limit(2).out().map(__.identity()).map(__.identity()).limit(1), __.out().limit(2).map(__.identity()).map(__.identity()).out().limit(1).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).limit(2).map(__.identity()).map(__.identity()).limit(1), __.out().limit(1).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(5, 20).map(__.identity()).map(__.identity()).range(5, 10), __.out().range(10, 15).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, 50), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, 60), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, -1).map(__.identity()).map(__.identity()).range(10, 60), __.out().range(60, 110).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(10, -1), __.out().range(60, 100).map(__.identity()).map(__.identity()).map(__.identity()).map(__.identity()), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).as("a").map(__.identity()).map(__.identity()).range(10, -1).as("b"), __.out().range(60, 100).map(__.identity()).map(__.identity()).as("a").map(__.identity()).map(__.identity()).as("b"), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(50, -1), __.out().discard(), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).map(__.identity()).map(__.identity()).range(60, -1), __.out().discard(), Collections.emptyList()},
                {__.out().map(__.identity()).map(__.identity()).range(50, 100).as("a").map(__.identity()).map(__.identity()).range(60, -1).as("b"), __.out().discard(), Collections.emptyList()},
                {__.out().range(50, 100).store("a").range(50, -1), __.out().range(50, 100).store("a").discard(), Collections.emptyList()},
                {__.out().range(50, 100).store("a").range(50, -1).cap("a"), ((GraphTraversal) __.out().range(50, 100).store("a").discard()).cap("a"), Collections.emptyList()},
                {__.out().range(50, 100).map(__.identity()).range(50, -1).profile(), __.out().discard().profile(), Collections.singleton(ProfileStrategy.instance())},
                {__.out().store("a").limit(10), __.out().limit(10).store("a"), Collections.emptyList()},
                {__.out().aggregate("a").limit(10), __.out().aggregate("a").limit(10), Collections.emptyList()},
                {__.V().branch(__.label()).option("person", __.out("knows").valueMap().limit(1)).option("software", __.out("created").valueMap().limit(2).fold()),
                 __.V().branch(__.label()).option("person", __.out("knows").limit(1).valueMap()).option("software", __.out("created").limit(2).valueMap().fold()), Collections.emptyList()}
        });
    }
}
