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

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeStepPlaceholder.RangeGlobalStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides utilities to verify the state and behavior of {@code GValueManager} during and after traversal strategy
 * application. It offers a builder pattern to configure and perform verification checks for traversals.
 * Multiple strategies can be applied in the order they are provided.
 */
public class GValueManagerVerifier {

    /**
     * Creates a verification builder for the given traversal and strategies
     */
    public static <S, E> VerificationBuilder<S, E> verify(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy) {
        return verify(traversal, strategy, Collections.emptySet());
    }

    /**
     * Creates a verification builder for the given traversal and strategies
     */
    public static <S, E> VerificationBuilder<S, E> verify(final Traversal.Admin<S, E> traversal, final TraversalStrategy strategy,
                                                          final Collection<TraversalStrategy> additionalStrategies) {
        // Create an array with FilterRankingStrategy as the first strategy
        TraversalStrategy[] strategies;

        if (additionalStrategies.isEmpty()) {
            // If no additional strategies, just use the one provided
            strategies = new TraversalStrategy[] { strategy };
        } else {
            // If there are additional strategies, combine them with one provided
            strategies = new TraversalStrategy[additionalStrategies.size() + 1];
            strategies[0] = strategy;

            int i = 1;
            for (TraversalStrategy ts : additionalStrategies) {
                strategies[i++] = ts;
            }
        }

        return new VerificationBuilder<>(traversal, strategies);
    }

    /**
     * Builder for configuring verification parameters
     */
    public static class VerificationBuilder<S, E> {
        private final Traversal.Admin<S, E> traversal;
        private final TraversalStrategy[] strategies;

        private VerificationBuilder(final Traversal.Admin<S, E> traversal, final TraversalStrategy... strategies) {
            this.traversal = traversal;
            this.strategies = strategies;
        }

        public BeforeVerifier<S, E> beforeApplying() {
            return new BeforeVerifier<>(traversal, this);
        }

        public AfterVerifier<S, E> afterApplying() {
            // Capture pre-strategy state
            final GValueManager manager = traversal.getGValueManager();
            final Map<Step, Collection<GValue<?>>> preStepGValues = TraversalHelper.gatherStepGValues(traversal);
            final Set<String> preVariables = manager.getVariableNames();
            final Set<GValue<?>> preGValues = manager.getGValues();

            // Apply strategies
            final TraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
            for (TraversalStrategy strategy : strategies) {
                traversalStrategies.addStrategies(strategy);
            }
            traversal.setStrategies(traversalStrategies);
            traversal.applyStrategies();

            return new AfterVerifier<>(traversal, preVariables, preStepGValues, preGValues);
        }
    }

    /**
     * Provides verification methods before strategy applications
     */
    public static class BeforeVerifier<S, E> extends AbstractVerifier<S, E, BeforeVerifier<S, E>> {
        private final VerificationBuilder<S, E> verificationBuilder;

        private BeforeVerifier(final Traversal.Admin<S, E> traversal, final VerificationBuilder<S, E> verificationBuilder) {
            super(traversal);
            this.verificationBuilder = verificationBuilder;
        }

        @Override
        protected BeforeVerifier<S, E> self() {
            return this;
        }

        /**
         * Applies the strategy and returns the verifier
         */
        public AfterVerifier<S, E> afterApplying() {
            return verificationBuilder.afterApplying();
        }
    }

    /**
     * Provides verification methods after strategy application
     */
    public static class AfterVerifier<S, E> extends AbstractVerifier<S, E, AfterVerifier<S, E>> {
        protected final Set<String> preVariables;
        protected final Set<GValue<?>> preGValues;
        protected final Map<Step, Collection<GValue<?>>> preStepGValues;
        protected final Map<Step, Set<String>> preStepVariables;

        private AfterVerifier(final Traversal.Admin<S, E> traversal,
                              final Set<String> preVariables,
                              final Map<Step, Collection<GValue<?>>> preStepGValues,
                              final Set<GValue<?>> preGValues) {
            super(traversal);
            this.preVariables = preVariables;
            this.preStepGValues = preStepGValues;
            this.preGValues = preGValues;

            // compute the preStepVariables from the preStepGValues by converting the GValue to their name if
            // their isVariable is true
            this.preStepVariables = preStepGValues.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().stream()
                                    .filter(GValue::isVariable)
                                    .map(GValue::getName)
                                    .collect(java.util.stream.Collectors.toSet()),
                            (v1, v2) -> {
                                v1.addAll(v2);
                                return v1;
                            },
                            IdentityHashMap::new
                    ));
        }

        @Override
        protected AfterVerifier<S, E> self() {
            return this;
        }

        /**
         * Verifies that all variables are preserved
         */
        public AfterVerifier<S, E> variablesArePreserved() {
            final Set<String> currentVariables = manager.getVariableNames();
            assertEquals("All variables should be preserved", preVariables, currentVariables);
            return this;
        }

        /**
         * Verifies that the contents of the GValueManager before and after applying strategies are unchanged.
         * This ensures that they have the same Step references, StepContract objects, and the contents of the
         * StepContracts are also the same.
         */
        public AfterVerifier<S, E> notModified() {
            // Verify that the same steps are in the manager
            // Use identity-based sets for Step objects
            final Set<Step> preSteps = Collections.newSetFromMap(new IdentityHashMap<>());
            preSteps.addAll(preStepGValues.keySet());

            final Set<Step> currentSteps = Collections.newSetFromMap(new IdentityHashMap<>());
            currentSteps.addAll(TraversalHelper.gatherGValuePlaceholders(traversal));

            assertEquals("Steps in GValueManager should be unchanged", preSteps.size(), currentSteps.size());

            for (Step preStep : preSteps) {
                boolean found = false;
                for (Step currentStep : currentSteps) {
                    if (preStep.toString().equals(currentStep.toString())) {
                        found = true;

                        // Verify that the step has the same GValues
                        final Collection<GValue<?>> preGValuesForStep = preStepGValues.get(preStep);
                        final Collection<GValue<?>> currentGValuesForStep = currentStep instanceof GValueHolder ?
                                ((GValueHolder<?, ?>) currentStep).getGValues() :
                                Collections.emptySet();
                        assertEquals("GValues for step should be unchanged", preGValuesForStep, currentGValuesForStep);

                        // Verify that the step has the same variables
                        final Set<String> preVariablesForStep = preStepVariables.get(preStep);
                        if (preVariablesForStep != null) {
                            final Set<String> currentVariablesForStep = currentGValuesForStep.stream()
                                    .filter(GValue::isVariable)
                                    .map(GValue::getName)
                                    .collect(Collectors.toSet());
                            assertEquals("Variables for step should be unchanged", preVariablesForStep, currentVariablesForStep);
                        }

                        // TODO:: Verify that the GValues are the same if they exist
                        break;
                    }
                }
                assertTrue("Step not found in current steps: " + preStep, found);
            }

            // Verify that the same GValues are in the manager
            final Set<GValue<?>> currentGValues = manager.getGValues();
            assertEquals("GValues in GValueManager should be unchanged", preGValues, currentGValues);

            return this;
        }
    }

    /**
     * Abstract base class for verification methods
     */
    public static abstract class AbstractVerifier<S, E, T extends AbstractVerifier<S, E, T>> {
        protected final Traversal.Admin<S, E> traversal;
        protected final GValueManager manager;

        protected AbstractVerifier(final Traversal.Admin<S, E> traversal) {
            this.traversal = traversal;
            this.manager = traversal.getGValueManager();
        }

        /**
         * Returns this instance cast to the implementing class type.
         */
        protected abstract T self();

        /**
         * Verifies that GValueManager is empty, specifically meaning that there are no variables in the manager. There
         * might yet be unnamed {@link GValue} instances but since we don't bind to those and they are usually incident
         * to something like {@code V(1, x=2, 3)} where one variable implies that all arguments must be converted to
         * {@link GValue} you can have scenarios where there is a mix.
         */
        public T managerIsEmpty() {
            assertThat(String.format("All GValues should be pinned, but contains unpinned variables [%s]", manager.getUnpinnedVariableNames()), manager.hasUnpinnedVariables(), is(false));
            List<GValueHolder> gValueSteps = TraversalHelper.getStepsOfAssignableClassRecursively(GValueHolder.class, traversal);

            List<GValueHolder> parameterizedSteps = gValueSteps.stream().filter(GValueHolder::isParameterized).collect(Collectors.toList());
            assertThat(String.format("No parameterized steps should be in the traversal, but contains [%s]", parameterizedSteps), parameterizedSteps.isEmpty());

            // HasContainerHolder is the only permitable GValueHolder to remain, as only the predicate is reduced not the step itself.
            gValueSteps = gValueSteps.stream().filter((s) -> !(s instanceof HasContainerHolder)).collect(Collectors.toList());
            assertThat(String.format("No GValueHolders should be in the traversal except for HasContainerHolder, but contains [%s]", gValueSteps), gValueSteps.isEmpty());
            return self();
        }

        /**
         * Verifies that and GValues in GValueManager have been pinned to indicate that optimizations have been applied
         * which cannot be generalized for any parameter values. Pinning of GValues only applies to variables. There
         * might yet be unnamed {@link GValue} instances which are unpinned, but since we don't bind to those and they
         * are usually incident to something like {@code V(1, x=2, 3)} where one variable implies that all arguments
         * must be converted to {@link GValue} you can have scenarios where there is a mix.
         */
        public T allGValuesArePinned() {
            assertThat(String.format("GValueManager only contain pinned GValues but [%s] is unpinned", manager.getUnpinnedVariableNames()), manager.hasUnpinnedVariables(), is(false));
            return self();
        }

        /**
         * Verifies whether steps are parameterized
         */
        public T stepsAreParameterized(final boolean isParameterized, final Step... steps) {
            assertThat("At least one step must be provided", steps.length > 0, is(true));
            for (Step step : steps) {
                assertThat("Step should not be parameterized", step instanceof GValueHolder && ((GValueHolder<?, ?>) step).isParameterized(), is(isParameterized));
            }
            return self();
        }

        /**
         * Verifies whether all steps of a certain class are parameterized
         */
        public <U extends Step> T stepsOfClassAreParameterized(final boolean isParameterized, final Class<U> stepClass) {
            final List<U> steps = TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, traversal);
            return stepsAreParameterized(isParameterized, steps.toArray(new Step[steps.size()]));
        }

        /**
         * Verifies that a RangeGlobalStepPlaceholder is properly set up
         */
        public T isRangeGlobalStepPlaceholder(final Step step, final long expectedLow, final long expectedHigh,
                                              final String lowName, final String highName) {
            assertThat("Step should be parameterized", step instanceof RangeGlobalStepPlaceholder && ((RangeGlobalStepPlaceholder<?>) step).isParameterized(), is(true));

            RangeGlobalStepPlaceholder<?> rangeGlobalStepPlaceholder = (RangeGlobalStepPlaceholder<?>) step;

            assertEquals("Low range should match", expectedLow, rangeGlobalStepPlaceholder.getLowRangeGValueSafe());
            assertEquals("High range should match", expectedHigh, rangeGlobalStepPlaceholder.getHighRangeGValueSafe());
            assertEquals("Low range name should match", lowName, rangeGlobalStepPlaceholder.getLowName());
            assertEquals("High range name should match", highName, rangeGlobalStepPlaceholder.getHighName());

            return self();
        }

        /**
         * Verifies that a VertexStepPlaceholder is properly set up
         */
        public T isVertexStepPlaceholder(final Step step, final int expectedLabelCount, final Set<String> expectedNames,
                                              final Set<String> expectedValues) {
            assertThat("Step should be parameterized", step instanceof VertexStepPlaceholder && ((VertexStepPlaceholder<?>) step).isParameterized(), is(true));

            VertexStepPlaceholder<?> vertexStepPlaceholder = (VertexStepPlaceholder<?>) step;

            assertEquals("Label count should match", expectedLabelCount, vertexStepPlaceholder.getEdgeLabelsGValueSafe().length);
            assertTrue("Expected names should match", CollectionUtils.isEqualCollection(expectedNames, vertexStepPlaceholder.getGValues().stream().map(GValue::getName).collect(Collectors.toSet())));
            assertTrue("Expected values should match", CollectionUtils.isEqualCollection(expectedValues, Set.of(vertexStepPlaceholder.getEdgeLabelsGValueSafe())));

            return self();
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasVariables(final String... variables) {
            return hasVariables(CollectionUtil.asSet(variables));
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasVariables(final Set<String> variables) {
            // Get variables from the current traversal and all child traversals
            final Set<String> currentVariables = manager.getVariableNames();
            assertEquals("Variables should match expected set", variables, currentVariables);
            return self();
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasUnpinnedVariables(final String... variables) {
            return hasUnpinnedVariables(CollectionUtil.asSet(variables));
        }

        /**
         * Verifies that specific variables exist.
         */
        public T hasUnpinnedVariables(final Set<String> variables) {
            // Get variables from the current traversal and all child traversals
            final Set<String> currentVariables = manager.getUnpinnedVariableNames();
            assertEquals("Unpinned variables should match expected set", variables, currentVariables);
            return self();
        }
    }
}
