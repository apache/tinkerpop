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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.P;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.structure.P.*;
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
        public String name;

        @Parameterized.Parameter(value = 1)
        public Object predicate;

        @Parameterized.Parameter(value = 2)
        public long expectedHighRange;

        @Before
        public void setup() {
            this.traversalEngine = mock(TraversalEngine.class);
            when(this.traversalEngine.getType()).thenReturn(TraversalEngine.Type.STANDARD);
        }

        @Test
        public void shouldApplyStrategy() {
            doTest(predicate, expectedHighRange);
        }
    }

    @RunWith(Parameterized.class)
    public static class ComputerTest extends AbstractRangeByIsCountStrategyTest {

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return generateTestParameters();
        }

        @Parameterized.Parameter(value = 0)
        public String name;

        @Parameterized.Parameter(value = 1)
        public Object predicate;

        @Parameterized.Parameter(value = 2)
        public long expectedHighRange;

        @Before
        public void setup() {
            this.traversalEngine = mock(TraversalEngine.class);
            when(this.traversalEngine.getType()).thenReturn(TraversalEngine.Type.COMPUTER);
        }

        @Test
        public void shouldApplyStrategy() {
            doTest(predicate, expectedHighRange);
        }
    }

    public static class SpecificComputerTest extends AbstractRangeByIsCountStrategyTest {

        @Before
        public void setup() {
            this.traversalEngine = mock(TraversalEngine.class);
            when(this.traversalEngine.getType()).thenReturn(TraversalEngine.Type.COMPUTER);
        }

        @Test
        public void nestedCountEqualsNullShouldLimitToOne() {
            final AtomicInteger counter = new AtomicInteger(0);
            final Traversal traversal = __.out().has(__.outE("created").count().is(0));
            applyRangeByIsCountStrategy(traversal);

            final HasTraversalStep hasStep = TraversalHelper.getStepsOfClass(HasTraversalStep.class, traversal.asAdmin()).stream().findFirst().get();
            final Traversal nestedTraversal = (Traversal) hasStep.getLocalChildren().get(0);
            TraversalHelper.getStepsOfClass(RangeGlobalStep.class, nestedTraversal.asAdmin()).stream().forEach(step -> {
                assertEquals(0, step.getLowRange());
                assertEquals(1, step.getHighRange());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.get());
        }
    }

    private static abstract class AbstractRangeByIsCountStrategyTest {

        protected TraversalEngine traversalEngine;

        void applyRangeByIsCountStrategy(final Traversal traversal) {
            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(RangeByIsCountStrategy.instance());

            traversal.asAdmin().setStrategies(strategies);
            traversal.asAdmin().applyStrategies();
            traversal.asAdmin().setEngine(traversalEngine);
        }

        public void doTest(final Object predicate, final long expectedHighRange) {
            final AtomicInteger counter = new AtomicInteger(0);
            final Traversal traversal;
            if (predicate instanceof P) {
                traversal = __.out().count().is(new P[]{(P) predicate});
            } else if (predicate instanceof P[]) {
                traversal = __.out().count().is((P[]) predicate);
            } else {
                traversal = __.out().count().is(predicate);
            }
            applyRangeByIsCountStrategy(traversal);

            final List<RangeGlobalStep> steps = TraversalHelper.getStepsOfClass(RangeGlobalStep.class, traversal.asAdmin());
            assertEquals(1, steps.size());

            steps.forEach(step -> {
                assertEquals(0, step.getLowRange());
                assertEquals(expectedHighRange, step.getHighRange());
                counter.incrementAndGet();
            });

            assertEquals(1, counter.intValue());
        }

        static Iterable<Object[]> generateTestParameters() {

            return Arrays.asList(new Object[][]{
                    {"countEqualsNullShouldLimitToOne", eq(0l), 1l},
                    {"countNotEqualsFourShouldLimitToFive", neq(4l), 5l},
                    {"countLessThanOrEqualThreeShouldLimitToFour", lte(3l), 4l},
                    {"countLessThanThreeShouldLimitToThree", lt(3l), 3l},
                    {"countGreaterThanTwoShouldLimitToThree", gt(2l), 3l},
                    {"countGreaterThanOrEqualTwoShouldLimitToTwo", gte(2l), 2l},
                    {"countInsideTwoAndFourShouldLimitToFour", inside(2l, 4l), 4l},
                    {"countOutsideTwoAndFourShouldLimitToFive", outside(2l, 4l), 5l},
                    {"countWithinTwoSixFourShouldLimitToSeven", within(2l, 6l, 4l), 7l},
                    {"countWithoutTwoSixFourShouldLimitToSix", without(2l, 6l, 4l), 6l}});
        }
    }
}
