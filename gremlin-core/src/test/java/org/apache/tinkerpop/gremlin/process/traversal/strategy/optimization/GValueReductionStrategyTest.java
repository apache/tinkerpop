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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GValueReductionStrategyTest {

    private static void applyAndAssertOneForOne(final Traversal.Admin<?, ?> traversal) {
        final List<GValueHolder> prePlaceholders = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);
        assertTrue("Expected GValueHolder steps before applying strategy", !prePlaceholders.isEmpty());
        final int preTopLevelStepCount = traversal.getSteps().size();

        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(GValueReductionStrategy.instance());
        traversal.setStrategies(strategies);
        traversal.applyStrategies();

        final List<GValueHolder> postPlaceholders = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);
        assertThat("All GValueHolder steps should be reduced after applying strategy", postPlaceholders.isEmpty(), is(true));
        final int postTopLevelStepCount = traversal.getSteps().size();
        assertEquals("Top-level step count should remain the same after reduction", preTopLevelStepCount, postTopLevelStepCount);
    }

    @Test
    public void shouldReduceGValueHoldersInRootTraversalOneForOne() {
        // Build a traversal that will contain GValueHolder placeholders (IsStepPlaceholder, RangeGlobalStepPlaceholder)
        final Traversal.Admin<?, ?> traversal = __.identity()
                .is(P.eq(GValue.of("x", 1)))
                .range(GValue.of("l", 0L), GValue.of("h", 10L))
                .asAdmin();

        // Pre-condition: there should be GValueHolder steps present
        final List<GValueHolder> prePlaceholders = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);
        assertTrue("Expected GValueHolder steps before applying strategy", !prePlaceholders.isEmpty());

        // Capture step count before applying the strategy (top-level traversal only)
        final int preStepCount = traversal.getSteps().size();

        // Apply and assert
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceGValueHoldersInChildTraversalsOneForOne() {
        // Build a traversal with a child traversal that includes placeholders and add a root placeholder as well
        final Traversal.Admin<?, ?> traversal = __.map(__.identity().is(P.eq(GValue.of("y", 2))))
                .tail(GValue.of("t", 5L))
                .asAdmin();

        // Pre-condition: there should be GValueHolder steps present recursively (child + root)
        final List<GValueHolder> prePlaceholders = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);
        assertTrue("Expected GValueHolder steps before applying strategy", !prePlaceholders.isEmpty());

        // Capture step counts before applying the strategy
        final int preTopLevelStepCount = traversal.getSteps().size();

        // Also capture the child traversal step count for the TraversalMapStep
        final TraversalMapStep<?, ?> mapStepBefore = TraversalHelper.getFirstStepOfAssignableClass(TraversalMapStep.class, traversal).get();
        final Traversal.Admin<?, ?> childBefore = mapStepBefore.getLocalChildren().get(0);
        final int preChildStepCount = childBefore.getSteps().size();

        // Apply GValueReductionStrategy
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(GValueReductionStrategy.instance());
        traversal.setStrategies(strategies);
        traversal.applyStrategies();

        // Post-condition: there should be no GValueHolder steps remaining recursively
        final List<GValueHolder> postPlaceholders = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);
        assertThat("All GValueHolder steps should be reduced in both root and child traversals", postPlaceholders.isEmpty(), is(true));

        // Validate one-for-one replacement: top-level step count unchanged
        final int postTopLevelStepCount = traversal.getSteps().size();
        assertEquals("Top-level step count should remain the same after reduction", preTopLevelStepCount, postTopLevelStepCount);

        // Validate one-for-one replacement: child traversal step count unchanged
        final TraversalMapStep<?, ?> mapStepAfter = TraversalHelper.getFirstStepOfAssignableClass(TraversalMapStep.class, traversal).get();
        final Traversal.Admin<?, ?> childAfter = mapStepAfter.getLocalChildren().get(0);
        final int postChildStepCount = childAfter.getSteps().size();
        assertEquals("Child traversal step count should remain the same after reduction", preChildStepCount, postChildStepCount);

        // Sanity: ensure that TailGlobalStep is concrete after reduction
        assertThat("Tail step should be concrete after reduction",
                TraversalHelper.getFirstStepOfAssignableClass(TailGlobalStep.class, traversal).isPresent(), is(true));
    }

    @Test
    public void shouldReduceGraphStepPlaceholderV() {
        final Traversal.Admin<?, ?> traversal = __.V(GValue.of("id", 1)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceGraphStepPlaceholderE() {
        final Traversal.Admin<?, ?> traversal = __.E(GValue.of("id", 2)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceOutWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.out(GValue.of("l", "knows")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceInWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.in(GValue.of("l", "created")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceBothWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.both(GValue.of("l", "uses")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceToDirectionWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.to(org.apache.tinkerpop.gremlin.structure.Direction.OUT, GValue.of("l", "knows")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceOutEWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.outE(GValue.of("l", "knows")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceInEWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.inE(GValue.of("l", "created")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceBothEWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.bothE(GValue.of("l", "uses")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceToEWithGValueLabels() {
        final Traversal.Admin<?, ?> traversal = __.toE(org.apache.tinkerpop.gremlin.structure.Direction.IN, GValue.of("l", "knows")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceRangeGlobalWithGValues() {
        final Traversal.Admin<?, ?> traversal = __.range(GValue.of("low", 1L), GValue.of("high", 3L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceRangeLocalWithGValues() {
        final Traversal.Admin<?, ?> traversal = __.range(org.apache.tinkerpop.gremlin.process.traversal.Scope.local,
                GValue.of("low", 0L), GValue.of("high", 2L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceLimitGlobalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.limit(GValue.of("lim", 5L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceLimitLocalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.limit(org.apache.tinkerpop.gremlin.process.traversal.Scope.local, GValue.of("lim", 2L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceSkipGlobalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.skip(GValue.of("sk", 7L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceSkipLocalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.skip(org.apache.tinkerpop.gremlin.process.traversal.Scope.local, GValue.of("sk", 3L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceTailGlobalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.tail(GValue.of("t", 4L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceTailLocalWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.tail(org.apache.tinkerpop.gremlin.process.traversal.Scope.local, GValue.of("t", 1L)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceAddVWithGValueLabel() {
        final Traversal.Admin<?, ?> traversal = __.addV(GValue.of("vl", "person")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceAddEWithGValueLabel() {
        final Traversal.Admin<?, ?> traversal = __.addE(GValue.of("el", "knows")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceAddPropertyWithGValue() {
        final Traversal.Admin<?, ?> traversal = __.addV("x").property("name", GValue.of("n", "marko")).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

//    @Test
//    public void shouldReduceCallWithGValueParams() {
//        final java.util.Map<String, Object> params = java.util.Map.of("a", 1);
//        final Traversal.Admin<?, ?> traversal = __.call("svc", GValue.of("p", params)).asAdmin();
//        applyAndAssertOneForOne(traversal);
//    }

    @Test
    public void shouldReduceMergeVWithGValue() {
        final java.util.Map<Object, Object> merge = java.util.Map.of("name", "x");
        final Traversal.Admin<?, ?> traversal = __.mergeV(GValue.of("m", merge)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }

    @Test
    public void shouldReduceMergeEWithGValue() {
        final java.util.Map<Object, Object> merge = java.util.Map.of("label", "knows");
        final Traversal.Admin<?, ?> traversal = __.mergeE(GValue.of("m", merge)).asAdmin();
        applyAndAssertOneForOne(traversal);
    }
}
