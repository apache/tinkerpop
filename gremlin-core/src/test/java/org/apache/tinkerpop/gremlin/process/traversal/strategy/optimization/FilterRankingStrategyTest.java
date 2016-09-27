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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class FilterRankingStrategyTest {

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
        strategies.addStrategies(FilterRankingStrategy.instance());
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
                {__.dedup().order(), __.dedup().order(), Collections.emptyList()},
                {__.order().dedup(), __.dedup().order(), Collections.emptyList()},
                {__.order().as("a").dedup(), __.order().as("a").dedup(), Collections.emptyList()},
                {__.identity().order().dedup(), __.dedup().order(), Collections.singletonList(IdentityRemovalStrategy.instance())},
                {__.order().identity().dedup(), __.dedup().order(), Collections.singletonList(IdentityRemovalStrategy.instance())},
                {__.order().out().dedup(), __.order().out().dedup(), Collections.emptyList()},
                {has("value", 0).filter(__.out()).dedup(), has("value", 0).filter(__.out()).dedup(), Collections.emptyList()},
                {__.dedup().filter(__.out()).has("value", 0), has("value", 0).filter(__.out()).dedup(), Collections.emptyList()},
                {__.filter(__.out()).dedup().has("value", 0), has("value", 0).filter(__.out()).dedup(), Collections.emptyList()},
                {has("value", 0).filter(__.out()).dedup(), has("value", 0).filter(__.out()).dedup(), Collections.emptyList()},
                {has("value", 0).or(has("name"), has("age")).has("value", 1).dedup(), has("value", 0).has("value", 1).or(has("name"), has("age")).dedup(), Collections.emptyList()},
                {has("value", 0).and(has("age"), has("name", "marko")).is(10), __.is(10).has("value", 0).has("name", "marko").has("age"), Collections.singletonList(InlineFilterStrategy.instance())},
        });
    }
}


