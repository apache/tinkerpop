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
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class RepeatUnrollStrategyTest {

    private static final Random RANDOM = new Random();
    private static boolean shouldBeFasterWithUniqueStreamExecuted = false;
    private static boolean shouldBeFasterWithDuplicateStreamExecuted = false;

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;


    private void applyRepeatUnrollStrategy(final Traversal traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(RepeatUnrollStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();

    }

    @Test
    public void doTest() {
        applyRepeatUnrollStrategy(original);
        assertEquals(optimized, original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        final Predicate<Traverser<Vertex>> predicate = t -> t.loops() > 5;
        return Arrays.asList(new Traversal[][]{
                {__.identity(), __.identity()},
                {__.out().as("a").in().repeat(__.outE("created").bothV()).times(2).in(), __.out().as("a").in().outE("created").bothV().barrier().outE("created").bothV().barrier().in()},
                {__.out().repeat(__.outE("created").bothV()).times(1).in(), __.out().outE("created").bothV().barrier().in()},
                {__.repeat(__.outE("created").bothV()).times(1).in(), __.outE("created").bothV().barrier().in()},
                {__.repeat(__.out()).times(2).as("x").repeat(__.in().as("b")).times(3), __.out().barrier().out().barrier().as("x").in().as("b").barrier().in().as("b").barrier().in().as("b").barrier()},
                {__.repeat(__.outE("created").inV()).times(2), __.outE("created").inV().barrier().outE("created").inV().barrier()},
                {__.repeat(__.out()).times(3), __.out().barrier().out().barrier().out().barrier()},
                {__.repeat(__.local(__.select("a").out("knows"))).times(2), __.local(__.select("a").out("knows")).barrier().local(__.select("a").out("knows")).barrier()},
                {__.<Vertex>times(2).repeat(__.out()), __.out().barrier().out().barrier()},
                {__.<Vertex>out().times(2).repeat(__.out().as("a")).as("x"), __.out().out().as("a").barrier().out().as("a").barrier().as("x")},
                {__.repeat(__.out()).emit().times(2), __.repeat(__.out()).emit().times(2)},
                {__.repeat(__.out()).until(predicate), __.repeat(__.out()).until(predicate)},
                {__.repeat(__.out()).until(predicate).repeat(__.out()).times(2), __.repeat(__.out()).until(predicate).out().barrier().out().barrier()},
                {__.repeat(__.union(__.both(), __.identity())).times(2).out(), __.union(__.both(), __.identity()).barrier().union(__.both(), __.identity()).barrier().out()},
        });
    }

    @Test
    public void shouldBeFasterWithUniqueStream() {
        if (shouldBeFasterWithUniqueStreamExecuted)
            return;
        shouldBeFasterWithUniqueStreamExecuted = true;
        final int startSize = 1000;
        final int clockRuns = 1000;
        final Integer[] starts = new Integer[startSize];
        for (int i = 0; i < startSize; i++) {
            starts[i] = i; // 1000 unique objects
        }
        assertEquals(startSize, new HashSet<>(Arrays.asList(starts)).size());
        clockTraversals(starts, clockRuns);
    }

    @Test
    public void shouldBeFasterWithDuplicateStream() {
        if (shouldBeFasterWithDuplicateStreamExecuted)
            return;
        shouldBeFasterWithDuplicateStreamExecuted = true;
        final int startSize = 1000;
        final int clockRuns = 1000;
        final int uniques = 100;
        final Integer[] starts = new Integer[startSize];
        for (int i = 0; i < startSize; i++) {
            starts[i] = i % uniques; // 100 unique objects
        }
        assertEquals(uniques, new HashSet<>(Arrays.asList(starts)).size());
        clockTraversals(starts, clockRuns);
    }

    private void clockTraversals(final Integer[] starts, final int clockRuns) {
        final int times = RANDOM.nextInt(4) + 2;
        assertTrue(times > 1 && times < 6);
        final Supplier<Long> original = () -> __.inject(starts).repeat(__.identity()).times(times).<Long>sum().next();

        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(RepeatUnrollStrategy.instance());
        final Supplier<Long> optimized = () -> {
            final Traversal<Integer, Long> traversal = __.inject(starts).repeat(__.identity()).times(times).sum();
            traversal.asAdmin().setStrategies(strategies);
            return traversal.next();
        };

        final Pair<Double, Long> originalResult;
        final Pair<Double, Long> optimizedResult;
        if (RANDOM.nextBoolean()) {
            originalResult = TimeUtil.clockWithResult(clockRuns, original);
            optimizedResult = TimeUtil.clockWithResult(clockRuns, optimized);
        } else {
            optimizedResult = TimeUtil.clockWithResult(clockRuns, optimized);
            originalResult = TimeUtil.clockWithResult(clockRuns, original);
        }

        // System.out.println(originalResult + "---" + optimizedResult);
        assertEquals(originalResult.getValue1(), optimizedResult.getValue1());
        assertTrue(originalResult.getValue0() > optimizedResult.getValue0());
    }
}
