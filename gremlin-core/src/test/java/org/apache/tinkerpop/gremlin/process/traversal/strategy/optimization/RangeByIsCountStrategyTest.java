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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.outside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class RangeByIsCountStrategyTest {

    @RunWith(Parameterized.class)
    public static class StandardTest extends AbstractRangeByIsCountStrategyTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return generateTestParameters();
        }

        @Parameterized.Parameter(value = 0)
        public Traversal original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Before
        public void setup() {
            this.traversalEngine = mock(TraversalEngine.class);
            when(this.traversalEngine.getType()).thenReturn(TraversalEngine.Type.STANDARD);
        }

        @Test
        public void shouldApplyStrategy() {
            doTest(original, optimized);
        }
    }

    @RunWith(Parameterized.class)
    public static class ComputerTest extends AbstractRangeByIsCountStrategyTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return generateTestParameters();
        }

        @Parameterized.Parameter(value = 0)
        public Traversal original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Before
        public void setup() {
            this.traversalEngine = mock(TraversalEngine.class);
            when(this.traversalEngine.getType()).thenReturn(TraversalEngine.Type.COMPUTER);
        }

        @Test
        public void shouldApplyStrategy() {
            doTest(original, optimized);
        }
    }

    private static abstract class AbstractRangeByIsCountStrategyTest {

        protected TraversalEngine traversalEngine;

        void applyAdjacentToIncidentStrategy(final Traversal traversal) {
            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(RangeByIsCountStrategy.instance());

            traversal.asAdmin().setStrategies(strategies);
            traversal.asAdmin().setEngine(this.traversalEngine);
            traversal.asAdmin().applyStrategies();
        }

        public void doTest(final Traversal traversal, final Traversal optimized) {
            applyAdjacentToIncidentStrategy(traversal);
            assertEquals(optimized, traversal);
        }

        static Iterable<Object[]> generateTestParameters() {

            return Arrays.asList(new Traversal[][]{
                    {__.out().count().is(0), __.not(__.out())},
                    {__.out().count().is(lt(1)), __.not(__.out())},
                    {__.out().count().is(lte(0)), __.not(__.out())},
                    {__.out().count().is(0).as("a"), __.out().limit(1).count().is(0).as("a")},
                    {__.out().count().as("a").is(0), __.out().limit(1).count().as("a").is(0)},
                    {__.out().count().is(neq(4)), __.out().limit(5).count().is(neq(4))},
                    {__.out().count().is(lte(3)), __.out().limit(4).count().is(lte(3))},
                    {__.out().count().is(lt(3)), __.out().limit(3).count().is(lt(3))},
                    {__.out().count().is(gt(2)), __.out().limit(3).count().is(gt(2))},
                    {__.out().count().is(gte(2)), __.out().limit(2).count().is(gte(2))},
                    {__.out().count().is(inside(2, 4)), __.out().limit(4).count().is(inside(2, 4))},
                    {__.out().count().is(outside(2, 4)), __.out().limit(5).count().is(outside(2, 4))},
                    {__.out().count().is(within(2, 6, 4)), __.out().limit(7).count().is(within(2, 6, 4))},
                    {__.out().count().is(without(2, 6, 4)), __.out().limit(6).count().is(without(2, 6, 4))}});
        }
    }
}
