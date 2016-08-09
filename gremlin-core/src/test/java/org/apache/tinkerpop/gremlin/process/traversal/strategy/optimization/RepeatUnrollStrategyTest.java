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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyPerformanceTest;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

@RunWith(Enclosed.class)
public class RepeatUnrollStrategyTest {

    @RunWith(Parameterized.class)
    public static class ParameterizedTests {

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
            final int maxBarrierSize = 5000;
            final Predicate<Traverser<Vertex>> predicate = t -> t.loops() > 5;
            return Arrays.asList(new Traversal[][]{
                    {__.repeat(__.out()).times(0), __.repeat(__.out()).times(0)},
                    {__.<Vertex>times(0).repeat(__.out()), __.<Vertex>times(0).repeat(__.out())},
                    {__.identity(), __.identity()},
                    {__.out().as("a").in().repeat(__.outE("created").bothV()).times(2).in(), __.out().as("a").in().outE("created").bothV().barrier(maxBarrierSize).outE("created").bothV().barrier(maxBarrierSize).in()},
                    {__.out().repeat(__.outE("created").bothV()).times(1).in(), __.out().outE("created").bothV().barrier(maxBarrierSize).in()},
                    {__.repeat(__.outE("created").bothV()).times(1).in(), __.outE("created").bothV().barrier(maxBarrierSize).in()},
                    {__.repeat(__.out()).times(2).as("x").repeat(__.in().as("b")).times(3), __.out().barrier(maxBarrierSize).out().barrier(maxBarrierSize).as("x").in().as("b").barrier(maxBarrierSize).in().as("b").barrier(maxBarrierSize).in().as("b").barrier(maxBarrierSize)},
                    {__.repeat(__.outE("created").inV()).times(2), __.outE("created").inV().barrier(maxBarrierSize).outE("created").inV().barrier(maxBarrierSize)},
                    {__.repeat(__.out()).times(3), __.out().barrier(maxBarrierSize).out().barrier(maxBarrierSize).out().barrier(maxBarrierSize)},
                    {__.repeat(__.local(__.select("a").out("knows"))).times(2), __.local(__.select("a").out("knows")).barrier(maxBarrierSize).local(__.select("a").out("knows")).barrier(maxBarrierSize)},
                    {__.<Vertex>times(2).repeat(__.out()), __.out().barrier(maxBarrierSize).out().barrier(maxBarrierSize)},
                    {__.<Vertex>out().times(2).repeat(__.out().as("a")).as("x"), __.out().out().as("a").barrier(maxBarrierSize).out().as("a").barrier(maxBarrierSize).as("x")},
                    {__.repeat(__.out()).emit().times(2), __.repeat(__.out()).emit().times(2)},
                    {__.repeat(__.out()).until(predicate), __.repeat(__.out()).until(predicate)},
                    {__.repeat(__.out()).until(predicate).repeat(__.out()).times(2), __.repeat(__.out()).until(predicate).out().barrier(maxBarrierSize).out().barrier(maxBarrierSize)},
                    {__.repeat(__.union(__.both(), __.identity())).times(2).out(), __.union(__.both(), __.identity()).barrier(maxBarrierSize).union(__.both(), __.identity()).barrier(maxBarrierSize).out()},
                    {__.in().repeat(__.out("knows")).times(3).as("a").count().is(0), __.in().out("knows").barrier(maxBarrierSize).out("knows").barrier(maxBarrierSize).out("knows").as("a").count().is(0)},
            });
        }
    }

    public static class PerformanceTest extends TraversalStrategyPerformanceTest {

        @Override
        protected double getAssertionPercentile() {
            return 95.0;
        }

        @Override
        protected Class<? extends TraversalStrategy> getStrategyUnderTest() {
            return RepeatUnrollStrategy.class;
        }

        @Override
        protected Iterator<GraphTraversal> getTraversalIterator() {

            return new Iterator<GraphTraversal>() {

                final int minLoops = 2;
                final int maxLoops = 5;
                final int minModulo = 100;
                final int maxModulo = 1000;

                private int numberOfLoops = minLoops;
                private int modulo = minModulo;

                @Override
                public boolean hasNext() {
                    return modulo <= maxModulo;
                }

                @Override
                public GraphTraversal next() {
                    final Integer[] starts = IntStream.range(0, 1000).map(i -> i % modulo).boxed().toArray(Integer[]::new);
                    final GraphTraversal traversal = __.inject(starts).repeat(__.identity()).times(numberOfLoops).sum();
                    if (++numberOfLoops > maxLoops) {
                        numberOfLoops = minLoops;
                        modulo += 200;
                    }
                    return traversal;
                }
            };
        }
    }
}
