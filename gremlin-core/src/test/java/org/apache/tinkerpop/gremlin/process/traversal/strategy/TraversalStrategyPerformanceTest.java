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
package org.apache.tinkerpop.gremlin.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public abstract class TraversalStrategyPerformanceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TraversalStrategyPerformanceTest.class);
    private static final Random RANDOM = new Random();

    protected int getClockRuns() {
        return 1000;
    }

    /**
     * Specifies for how many percent of all comparisons the optimized traversal must be faster than the
     * original traversal. Default is 100.
     */
    protected double getAssertionPercentile() {
        return 95.0;
    }

    protected abstract Class<? extends TraversalStrategy> getStrategyUnderTest();

    protected TraversalStrategy<?> getStrategyUnderTestInstance() throws Exception {
        return (TraversalStrategy) getStrategyUnderTest().getMethod("instance").invoke(null);
    }

    protected abstract Iterator<GraphTraversal> getTraversalIterator();

    @Test
    public void shouldBeFaster() throws Exception {

        final TraversalStrategies withStrategyUnderTest = new DefaultTraversalStrategies();
        withStrategyUnderTest.addStrategies(getStrategyUnderTestInstance());

        final TraversalStrategies withoutStrategyUnderTest = new DefaultTraversalStrategies();
        withoutStrategyUnderTest.removeStrategies(getStrategyUnderTest());

        final int clockRuns = getClockRuns();
        final Iterator<GraphTraversal> iterator = getTraversalIterator();
        int faster = 0, numTraversals = 0;
        while (iterator.hasNext()) {

            final GraphTraversal traversal = iterator.next();
            final GraphTraversal.Admin original = traversal.asAdmin();
            final GraphTraversal.Admin optimized = original.clone();

            original.setStrategies(withoutStrategyUnderTest);
            optimized.setStrategies(withStrategyUnderTest);

            final double originalTime, optimizedTime;

            if (RANDOM.nextBoolean()) {
                originalTime = TimeUtil.clock(clockRuns, () -> original.clone().iterate());
                optimizedTime = TimeUtil.clock(clockRuns, () -> optimized.clone().iterate());
            } else {
                optimizedTime = TimeUtil.clock(clockRuns, () -> optimized.clone().iterate());
                originalTime = TimeUtil.clock(clockRuns, () -> original.clone().iterate());
            }

            final List originalResult = original.toList();
            final List optimizedResult = optimized.toList();

            if (originalTime > optimizedTime) {
                LOGGER.debug("Original traversal ({} ms): {}", originalTime, original);
                LOGGER.debug("Optimized traversal ({} ms): {}", optimizedTime, optimized);
            } else {
                LOGGER.warn("Original traversal ({} ms): {}", originalTime, original);
                LOGGER.warn("Optimized traversal ({} ms): {}", optimizedTime, optimized);
            }

            if (getAssertionPercentile() >= 100) assertTrue(originalTime > optimizedTime);
            else {
                if (originalTime > optimizedTime)
                    faster++;
                numTraversals++;
            }

            assertEquals(originalResult, optimizedResult);
        }

        if (getAssertionPercentile() < 100 && numTraversals > 0)
            assertTrue(((faster * 100.0) / numTraversals) >= getAssertionPercentile());
    }
}
