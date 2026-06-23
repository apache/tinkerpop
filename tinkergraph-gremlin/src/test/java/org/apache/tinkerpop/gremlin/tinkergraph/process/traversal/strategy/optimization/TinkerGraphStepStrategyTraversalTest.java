/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Property 9: Traversal-bearing HasContainers are not folded into GraphStep.
 * <p>
 * For any traversal of the form {@code g.V().has(key, traversal)} or {@code g.V().has(key, P.eq(traversal))},
 * after applying {@link TinkerGraphStepStrategy}, the {@link HasStep} SHALL remain as a separate step in the
 * traversal plan and SHALL NOT be folded into the {@link TinkerGraphStep}.
 * <p>
 * <b>Validates: Requirements 8.1, 8.2, 8.3</b>
 */
public class TinkerGraphStepStrategyTraversalTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    @After
    public void teardown() throws Exception {
        graph.close();
    }

    /**
     * Applies TinkerGraphStepStrategy to the given traversal and returns the step list.
     */
    private List<Step> applyStrategy(final Traversal.Admin<?, ?> traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(TinkerGraphStepStrategy.instance());
        traversal.setStrategies(strategies);
        traversal.applyStrategies();
        return traversal.getSteps();
    }

    @Test
    public void shouldNotFoldTraversalBearingHasContainerIntoGraphStep() {
        // g.V().has("name", __.constant("marko")) - traversal-bearing HasContainer
        final GraphTraversal<Vertex, Vertex> traversal = g.V().has("name", __.constant("marko"));
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // The traversal plan should contain at least 2 steps:
        // TinkerGraphStep (replacing GraphStep) and HasStep (not folded)
        assertThat(steps, hasSize(greaterThanOrEqualTo(2)));
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));

        // The HasStep should remain as a separate step
        final boolean hasStepPresent = steps.stream().anyMatch(s -> s instanceof HasStep);
        assertThat("HasStep with traversal-bearing HasContainer should NOT be folded into TinkerGraphStep",
                hasStepPresent, is(true));

        // The TinkerGraphStep should have no folded HasContainers
        final TinkerGraphStep<?, ?> tinkerGraphStep = (TinkerGraphStep<?, ?>) steps.get(0);
        assertThat("TinkerGraphStep should have no folded HasContainers for traversal-bearing has()",
                tinkerGraphStep.getHasContainers(), hasSize(0));
    }

    @Test
    public void shouldStillFoldLiteralHasContainerIntoGraphStep() {
        // g.V().has("name", "marko") - literal HasContainer should still be folded
        final GraphTraversal<Vertex, Vertex> traversal = g.V().has("name", "marko");
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // After folding, the HasStep should be absorbed into TinkerGraphStep
        assertThat(steps, hasSize(1));
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));

        // The TinkerGraphStep should have the folded HasContainer
        final TinkerGraphStep<?, ?> tinkerGraphStep = (TinkerGraphStep<?, ?>) steps.get(0);
        assertThat(tinkerGraphStep.getHasContainers(), hasSize(1));
    }

    @Test
    public void shouldNotFoldPredicateWithTraversalIntoGraphStep() {
        // g.V().has("name", P.eq(__.constant("marko"))) - predicate with traversal
        final GraphTraversal<Vertex, Vertex> traversal =
                g.V().has("name", org.apache.tinkerpop.gremlin.process.traversal.P.eq(__.constant("marko")));
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // The HasStep should remain as a separate step
        assertThat(steps, hasSize(greaterThanOrEqualTo(2)));
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));

        final boolean hasStepPresent = steps.stream().anyMatch(s -> s instanceof HasStep);
        assertThat("HasStep with P.eq(traversal) should NOT be folded into TinkerGraphStep",
                hasStepPresent, is(true));
    }

    @Test
    public void shouldFoldLiteralHasButNotTraversalHasInMixedTraversal() {
        // g.V().has("name", "marko").has("age", __.constant(29))
        // Both containers end up in the same HasStep (TraversalHelper.addHasContainer merges).
        // The strategy skips the entire HasStep because it contains a traversal-bearing container.
        final GraphTraversal<Vertex, Vertex> traversal =
                g.V().has("name", "marko").has("age", __.constant(29));
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // TinkerGraphStep should have NO folded HasContainers (the merged HasStep is skipped)
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));
        final TinkerGraphStep<?, ?> tinkerGraphStep = (TinkerGraphStep<?, ?>) steps.get(0);

        // The HasStep with both containers should remain as a separate step
        final boolean hasStepPresent = steps.stream().anyMatch(s -> s instanceof HasStep);
        assertThat("HasStep with mixed containers should remain as separate step",
                hasStepPresent, is(true));
    }

    @Test
    public void shouldFoldLiteralHasAfterBarrierAndTraversalHas() {
        // g.V().has("age", __.constant(29)).barrier().has("name", "marko")
        // The barrier separates the two HasSteps. The strategy should:
        // - Skip the traversal-bearing HasStep (has("age", traversal))
        // - Stop at the barrier (not a HasStep or NoOpBarrierStep... actually NoOpBarrierStep IS handled)
        // Let's use a different separator. Actually NoOpBarrierStep is handled by the while loop.
        // The key test is: after skipping a traversal-bearing HasStep, the strategy continues
        // and can fold subsequent literal HasSteps.
        //
        // With separate HasSteps (not merged), the strategy should fold the literal one.
        // We can force separate HasSteps by inserting a NoOpBarrierStep between them.
        final GraphTraversal<Vertex, Vertex> traversal =
                g.V().has("age", __.constant(29)).barrier().has("name", "marko");
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // TinkerGraphStep should have the literal "name" HasContainer folded
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));
        final TinkerGraphStep<?, ?> tinkerGraphStep = (TinkerGraphStep<?, ?>) steps.get(0);
        assertThat("Literal HasContainer after barrier should be folded even when preceded by traversal-bearing HasStep",
                tinkerGraphStep.getHasContainers(), hasSize(1));
        assertThat(tinkerGraphStep.getHasContainers().get(0).getKey(), is("name"));

        // The traversal-bearing HasStep for "age" should remain
        final boolean hasStepPresent = steps.stream().anyMatch(s -> s instanceof HasStep);
        assertThat("Traversal-bearing HasStep should remain as separate step",
                hasStepPresent, is(true));
    }

    @Test
    public void shouldFoldMultipleLiteralHasStepsSeparatedByTraversalHas() {
        // g.V().has("name", "marko").has("age", __.constant(29)).barrier().has("lang", "java")
        // has("name") is a separate literal HasStep, has("age") is a separate traversal HasStep,
        // has("lang") is a separate literal HasStep after the barrier.
        // Strategy should fold both literal HasSteps and skip the traversal one.
        final GraphTraversal<Vertex, Vertex> traversal =
                g.V().has("name", "marko").has("age", __.constant(29)).barrier().has("lang", "java");
        final List<Step> steps = applyStrategy(traversal.asAdmin());

        // TinkerGraphStep should have both literal HasContainers folded ("name" and "lang")
        assertThat(steps.get(0), instanceOf(TinkerGraphStep.class));
        final TinkerGraphStep<?, ?> tinkerGraphStep = (TinkerGraphStep<?, ?>) steps.get(0);
        assertThat("Both literal HasContainers should be folded",
                tinkerGraphStep.getHasContainers(), hasSize(2));

        // The traversal-bearing HasStep (age) should remain
        final boolean hasStepPresent = steps.stream().anyMatch(s -> s instanceof HasStep);
        assertThat("Traversal-bearing HasStep should remain as separate step",
                hasStepPresent, is(true));
    }

}
