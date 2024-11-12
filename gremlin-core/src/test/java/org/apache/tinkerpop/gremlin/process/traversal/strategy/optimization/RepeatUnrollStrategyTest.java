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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.path;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class RepeatUnrollStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin<?,?> original;

    @Parameterized.Parameter(value = 1)
    public Traversal<?,?> optimized;

    @Parameterized.Parameter(value = 2)
    public Collection<TraversalStrategy<?>> otherStrategies;

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        final TraversalStrategies strategiesWithLazy = new DefaultTraversalStrategies();
        strategiesWithLazy.addStrategies(RepeatUnrollStrategy.instance());
        Traversal.Admin<?, ?> clonedOriginal = this.original.clone();

        // adding LazyBarrierStrategy as RepeatUnrollStrategy adds barriers and the existence of this strategy
        // triggers those additions. if they are not there they will not be present and most of these assertions
        // assume this strategy present
        strategiesWithLazy.addStrategies(LazyBarrierStrategy.instance());
        for (final TraversalStrategy<?> strategy : this.otherStrategies) {
            strategiesWithLazy.addStrategies(strategy);
        }
        clonedOriginal.setStrategies(strategiesWithLazy);
        clonedOriginal.applyStrategies();
        assertEquals("With LazyBarrierStrategy: " + repr, this.optimized, clonedOriginal);

        final TraversalStrategies strategiesWithoutLazy = new DefaultTraversalStrategies();
        strategiesWithoutLazy.addStrategies(RepeatUnrollStrategy.instance());
        for (final TraversalStrategy<?> strategy : this.otherStrategies) {
            strategiesWithoutLazy.addStrategies(strategy);
        }
        clonedOriginal = this.original.clone();
        clonedOriginal.setStrategies(strategiesWithoutLazy);
        clonedOriginal.applyStrategies();
        final Traversal.Admin<?, ?> optimizedWithoutBarriers = this.optimized.asAdmin().clone();

        // remove all the barriers as LazyBarrierStrategy is not present and therefore RepeatUnrollStrategy should
        // not add any
        TraversalHelper.getStepsOfAssignableClassRecursively(NoOpBarrierStep.class, optimizedWithoutBarriers).forEach(s -> {
            TraversalHelper.copyLabels(s, s.getPreviousStep(), true);
            s.getTraversal().removeStep(s);
        });
        assertEquals("Without LazyBarrierStrategy: " + repr, optimizedWithoutBarriers, clonedOriginal);

    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        // tests added here should be written to assume that LazyBarrierStrategy is present. if a barrier() is
        // manually added as the "original" then include an exclusion in doTest()
        final int repeatBarrierSize = RepeatUnrollStrategy.MAX_BARRIER_SIZE;
        final Predicate<Traverser<Vertex>> predicate = t -> t.loops() > 5;
        return Arrays.asList(new Object[][]{
                {__.repeat(out()).times(0), __.repeat(out()).times(0), Collections.emptyList()},
                {__.<Vertex>times(0).repeat(out()), __.<Vertex>times(0).repeat(out()), Collections.emptyList()},
                {__.identity(), __.identity(), Collections.emptyList()},
                {out().as("a").in().repeat(__.outE("created").bothV()).times(2).in(), out().as("a").in().outE("created").bothV().barrier(repeatBarrierSize).outE("created").bothV().barrier(repeatBarrierSize).in(), Collections.emptyList()},
                {out().repeat(__.outE("created").bothV()).times(1).in(), out().outE("created").bothV().barrier(repeatBarrierSize).in(), Collections.emptyList()},
                {__.repeat(__.outE("created").bothV()).times(1).in(), __.outE("created").bothV().barrier(repeatBarrierSize).in(), Collections.emptyList()},
                {__.repeat(out()).times(2).as("x").repeat(__.in().as("b")).times(3), out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize).as("x").in().as("b").barrier(repeatBarrierSize).in().as("b").barrier(repeatBarrierSize).in().as("b").barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(__.outE("created").inV()).times(2), __.outE("created").inV().barrier(repeatBarrierSize).outE("created").inV().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(out()).times(3), out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(__.local(__.select("a").out("knows"))).times(2), __.local(__.select("a").out("knows")).barrier(repeatBarrierSize).local(__.select("a").out("knows")).barrier(repeatBarrierSize), Collections.emptyList()},
                {__.<Vertex>times(2).repeat(out()), out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.<Vertex>out().times(2).repeat(out().as("a")).as("x"), out().out().as("a").barrier(repeatBarrierSize).out().as("a").barrier(repeatBarrierSize).as("x"), Collections.emptyList()},
                {__.repeat(out()).emit().times(2), __.repeat(out()).emit().times(2), Collections.emptyList()},
                {__.repeat(out()).until(predicate), __.repeat(out()).until(predicate), Collections.emptyList()},
                {__.repeat(out()).until(predicate).repeat(out()).times(2), __.repeat(out()).until(predicate).out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(__.union(__.both(), __.identity())).times(2).out(), __.union(__.both(), __.identity()).barrier(repeatBarrierSize).union(__.both(), __.identity()).barrier(repeatBarrierSize).out(), Collections.emptyList()},
                {__.in().repeat(out("knows")).times(3).as("a").count().is(0), __.in().out("knows").barrier(repeatBarrierSize).out("knows").barrier(repeatBarrierSize).out("knows").as("a").count().is(0), Collections.emptyList()},
                //
                {__.repeat(__.outE().inV()).times(2), __.outE().inV().barrier(repeatBarrierSize).outE().inV().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(__.outE().filter(path()).inV()).times(2), __.outE().filter(path()).inV().barrier(repeatBarrierSize).outE().filter(path()).inV().barrier(repeatBarrierSize), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                {__.repeat(__.outE().inV()).times(2), __.out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize), Collections.singletonList(IncidentToAdjacentStrategy.instance())},
                // Nested Loop tests
                {__.repeat(out().repeat(out()).times(0)).times(1), __.out().repeat(out()).times(0).barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(out().repeat(out()).times(1)).times(1), __.out().out().barrier(repeatBarrierSize), Collections.emptyList()},
                {__.repeat(out()).until(__.repeat(out()).until(predicate)), __.repeat(out()).until(__.repeat(out()).until(predicate)), Collections.emptyList()},
                {__.repeat(__.repeat(out()).times(2)).until(predicate), __.repeat(__.out().barrier(repeatBarrierSize).out().barrier(repeatBarrierSize)).until(predicate), Collections.emptyList()},
                {__.repeat(__.repeat(out("created")).until(__.has("name", "ripple"))), __.repeat(__.repeat(out("created")).until(__.has("name", "ripple"))), Collections.emptyList()},
        });
    }
}
