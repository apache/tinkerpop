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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.otherV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class SubgraphStrategyTest {
    @RunWith(Parameterized.class)
    public static class TraverseTest {
        private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {__.bothV(), 1},
                    {__.inV(), 1},
                    {__.outV(), 1},
                    {__.in(), 3},
                    {__.in("test"), 3},
                    {__.both(), 3},
                    {__.both("test"), 3},
                    {__.out(), 3},
                    {__.out("test"), 3},
                    {__.out().bothE().otherV(), 5},
                    {g.addV(), 1},
                    {g.V(1).addE("test"), 2},
                    {__.in().out(), 6},
                    {__.out().out().out(), 9},
                    {__.in().out().in(), 9},
                    {__.inE().outV().inE().outV(), 4}});
        }

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin traversal;

        @Parameterized.Parameter(value = 1)
        public int expectedInsertedSteps;

        @Test
        public void shouldSubgraph() {
            final String repr = traversal.getGremlinLang().getGremlin();
            final SubgraphStrategy strategy = SubgraphStrategy.build().edges(__.has("edge")).vertices(__.has("vertex")).create();
            strategy.apply(traversal);

            final List<TraversalFilterStep> steps = TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal);
            assertEquals(repr, expectedInsertedSteps, steps.size());
        }
    }

    @RunWith(Parameterized.class)
    public static class MixedStrategiesTests {

        @Parameterized.Parameter(value = 0)
        public Traversal.Admin original;

        @Parameterized.Parameter(value = 1)
        public Traversal optimized;

        @Test
        public void doTest() {
            final String repr = original.getGremlinLang().getGremlin();
            final TraversalStrategies originalStrategies = new DefaultTraversalStrategies();
            originalStrategies.addStrategies(SubgraphStrategy.build().
                    vertices(__.and(has("name", "marko"), has("age", 29))).
                    edges(hasLabel("knows")).
                    vertexProperties(__.<VertexProperty, Long>values().count().and(is(P.lt(10)), is(0))).create());
            originalStrategies.addStrategies(InlineFilterStrategy.instance());
            originalStrategies.addStrategies(StandardVerificationStrategy.instance());
            this.original.asAdmin().setStrategies(originalStrategies);
            this.original.asAdmin().applyStrategies();
            final TraversalStrategies optimizedStrategies = new DefaultTraversalStrategies();
            optimizedStrategies.addStrategies(InlineFilterStrategy.instance());
            this.optimized.asAdmin().setStrategies(optimizedStrategies);
            this.optimized.asAdmin().applyStrategies();
            assertEquals(repr, this.optimized, this.original);
        }

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Traversal[][]{
                    {__.outE(), __.outE().hasLabel("knows").filter(inV().has("name", "marko").has("age", 29)) },
                    {__.inE(), __.inE().hasLabel("knows").filter(outV().has("name", "marko").has("age", 29)) },
                    {__.bothE(), __.bothE().hasLabel("knows").filter(otherV().has("name", "marko").has("age", 29)) },
                    {__.V(), __.V().has("name", "marko").has("age", 29)},
                    {__.V().has("location", "santa fe"), __.V().has("name", "marko").has("age", 29).has("location", "santa fe")},
                    {__.V().where(has("location", "santa fe")), __.V().has("name", "marko").has("age", 29).has("location", "santa fe")},
                    {__.V().where(has("location", "santa fe")).values("location"), __.V().has("name", "marko").has("age", 29).has("location", "santa fe").properties("location").filter(values().count().is(P.lt(10)).is(0)).value()}
            });
        }
    }

    public static class RewriteTest {

        @Test
        public void shouldAddFilterAfterVertex() {
            final SubgraphStrategy strategy = SubgraphStrategy.build().vertices(__.identity()).create();
            final Traversal t = __.inV();
            strategy.apply(t.asAdmin());
            final EdgeVertexStep edgeVertexStep = (EdgeVertexStep) t.asAdmin().getStartStep();
            assertEquals(TraversalFilterStep.class, edgeVertexStep.getNextStep().getClass());
            final TraversalFilterStep h = (TraversalFilterStep) t.asAdmin().getEndStep();
            assertEquals(1, h.getLocalChildren().size());
            assertThat(((DefaultGraphTraversal) h.getLocalChildren().get(0)).getEndStep(), CoreMatchers.instanceOf(IdentityStep.class));
        }

        @Test
        public void shouldAddFilterAfterEdge() {
            final SubgraphStrategy strategy = SubgraphStrategy.build().edges(__.identity()).create();
            final Traversal t = __.inE();
            strategy.apply(t.asAdmin());
            final VertexStep vertexStep = (VertexStep) t.asAdmin().getStartStep();
            assertEquals(TraversalFilterStep.class, vertexStep.getNextStep().getClass());
            final TraversalFilterStep h = (TraversalFilterStep) t.asAdmin().getEndStep();
            assertEquals(1, h.getLocalChildren().size());
            assertThat(((DefaultGraphTraversal) h.getLocalChildren().get(0)).getEndStep(), CoreMatchers.instanceOf(IdentityStep.class));
        }

        @Test
        public void shouldAddBothFiltersAfterVertex() {
            final SubgraphStrategy strategy = SubgraphStrategy.build().edges(__.identity()).vertices(__.identity()).create();
            final Traversal t = __.inE();
            strategy.apply(t.asAdmin());
            final VertexStep vertexStep = (VertexStep) t.asAdmin().getStartStep();
            assertEquals(TraversalFilterStep.class, vertexStep.getNextStep().getClass());
            final TraversalFilterStep h = (TraversalFilterStep) t.asAdmin().getEndStep();
            assertEquals(1, h.getLocalChildren().size());
            assertThat(((DefaultGraphTraversal) h.getLocalChildren().get(0)).getEndStep(), CoreMatchers.instanceOf(TraversalFilterStep.class));
        }

        @Test
        public void shouldNotRetainMarkers() {
            final SubgraphStrategy strategy = SubgraphStrategy.build().vertices(__.<Vertex>out().hasLabel("person")).create();
            final Traversal.Admin<?, ?> t = out().inE().asAdmin();
            t.setStrategies(t.getStrategies().clone().addStrategies(strategy, StandardVerificationStrategy.instance()));
            t.applyStrategies();
            assertEquals(VertexStep.class, t.getSteps().get(0).getClass());
            assertEquals(TraversalFilterStep.class, t.getSteps().get(1).getClass());
            assertEquals(VertexStep.class, ((TraversalFilterStep<?>) t.getSteps().get(1)).getLocalChildren().get(0).getStartStep().getClass());
            assertEquals(0, ((TraversalFilterStep<?>) t.getSteps().get(1)).getLocalChildren().get(0).getStartStep().getLabels().size());
            assertEquals(VertexStep.class, t.getSteps().get(2).getClass());
            assertEquals(TraversalFilterStep.class, t.getSteps().get(3).getClass());
            assertEquals(EdgeVertexStep.class, ((TraversalFilterStep<?>) t.getSteps().get(3)).getLocalChildren().get(0).getStartStep().getClass());
            assertEquals(0, ((TraversalFilterStep<?>) t.getSteps().get(3)).getLocalChildren().get(0).getStartStep().getLabels().size());
            TraversalHelper.getStepsOfAssignableClassRecursively(Step.class, t).forEach(step -> assertTrue(step.getLabels().isEmpty()));
        }
    }


}
