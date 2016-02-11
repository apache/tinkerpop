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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
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

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class RangeByIsCountStrategyTest {


    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Object predicate;

    @Parameterized.Parameter(value = 2)
    public long expectedHighRange;

    public void applyRangeByIsCountStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(RangeByIsCountStrategy.instance());

        traversal.asAdmin().setStrategies(strategies);
        //traversal.asAdmin().setEngine(this.traversalEngine);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        final AtomicInteger counter = new AtomicInteger(0);
        final Traversal traversal = __.out().count().is(predicate);

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

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

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