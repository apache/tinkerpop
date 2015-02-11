/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.tinkerpop.gremlin.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import com.tinkerpop.gremlin.process.graph.traversal.__;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.HasTraversalStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.RangeStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class RangeByIsCountStrategyTest extends AbstractGremlinProcessTest {

    public static class StandardTest extends RangeByIsCountStrategyTest {

        private void runTest(final BiPredicate predicate, final Object value, final long expectedHighRange) {
            final AtomicInteger counter = new AtomicInteger(0);
            Traversal traversal = g.V().count().is(predicate, value);
            traversal.iterate();
            TraversalHelper.getStepsOfClass(RangeStep.class, traversal.asAdmin()).stream().forEach(step -> {
                assertEquals(0, step.getLowRange());
                assertEquals(expectedHighRange, step.getHighRange());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.get());
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countEqualsNullShouldLimitToOne() {
            runTest(Compare.eq, 0l, 1l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countNotEqualsFourShouldLimitToFive() {
            runTest(Compare.neq, 4l, 5l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countLessThanOrEqualThreeShouldLimitToFour() {
            runTest(Compare.lte, 3l, 4l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countLessThanThreeShouldLimitToThree() {
            runTest(Compare.lt, 3l, 3l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countGreaterThanTwoShouldLimitToThree() {
            runTest(Compare.gt, 2l, 3l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countGreaterThanOrEqualTwoShouldLimitToTwo() {
            runTest(Compare.gte, 2l, 2l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countInsideTwoAndFourShouldLimitToFour() {
            runTest(Compare.inside, Arrays.asList(2l, 4l), 4l);
        }

        @Test
        @LoadGraphWith(MODERN)
        public void countOutsideTwoAndFourShouldLimitToFive() {
            runTest(Compare.outside, Arrays.asList(2l, 4l), 5l);
        }
    }

    public static class ComputerTest extends RangeByIsCountStrategyTest {
        @Test
        @LoadGraphWith(MODERN)
        public void nestedCountEqualsNullShouldLimitToOne() {
            final AtomicInteger counter = new AtomicInteger(0);
            final Traversal traversal = g.V().has(__.outE("created").count().is(0)).submit(g.compute()).iterate();
            final ComputerResultStep crs = (ComputerResultStep) traversal.asAdmin().getSteps().iterator().next();
            final Traversal ct = crs.getComputerTraversal();
            final HasTraversalStep hasStep = TraversalHelper.getStepsOfClass(HasTraversalStep.class, ct.asAdmin()).stream().findFirst().get();
            final Traversal nestedTraversal = (Traversal) hasStep.getLocalChildren().get(0);
            TraversalHelper.getStepsOfClass(RangeStep.class, nestedTraversal.asAdmin()).stream().forEach(step -> {
                assertEquals(0, step.getLowRange());
                assertEquals(1, step.getHighRange());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.get());
        }
    }
}
