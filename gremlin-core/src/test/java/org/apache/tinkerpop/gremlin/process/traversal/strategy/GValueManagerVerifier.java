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

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStepPlaceholder;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.EdgeLabelContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.StepContract;
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

        /**
         * Applies the strategies and returns the verifier while ensuring that the step state is consistent where
         * every step in the manager is in the traversal and every step that is a parameterized {@link StepContract}
         * is in the manager.
         */
        public AfterVerifier<S, E> afterApplying() {
            // Capture pre-strategy state
            final GValueManager manager = traversal.getGValueManager();
            final Map<Step, Set<GValue<?>>> preStepGValues = TraversalHelper.gatherStepGValues(traversal);
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
        protected final Map<Step, Set<GValue<?>>> preStepGValues;
        protected final Map<Step, Set<String>> preStepVariables;

        private AfterVerifier(final Traversal.Admin<S, E> traversal,
                              final Set<String> preVariables,
                              final Map<Step, Set<GValue<?>>> preStepGValues,
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
                        final Set<GValue<?>> preGValuesForStep = preStepGValues.get(preStep);
                        final Set<GValue<?>> currentGValuesForStep = currentStep instanceof GValueStepPlaceholder ?
                                ((GValueStepPlaceholder<?, ?>) currentStep).getGValues() :
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
            List<GValueStepPlaceholder> gValueSteps = TraversalHelper.getStepsOfAssignableClassRecursively(GValueStepPlaceholder.class, traversal);
            assertThat(String.format("No GValue placeholder steps should be in the traversal, but contains [%s]", gValueSteps), gValueSteps.isEmpty());
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
         * Verifies that specific steps have been cleared from GValueManager
         */
        public T stepsAreParameterized(final boolean isParameterized, final Step... steps) {
            assertThat("At least one step must be provided", steps.length > 0, is(true));
            for (Step step : steps) {
                final GValueManager manager = step.getTraversal().getGValueManager();
                assertThat("Step should not be parameterized", step instanceof GValueStepPlaceholder && ((GValueStepPlaceholder<?, ?>) step).isParameterized(), is(isParameterized));
            }
            return self();
        }

        /**
         * Verifies that all steps of a certain class are not parameterized
         */
        public <U extends Step> T stepsOfClassAreParameterized(final boolean isParameterized, final Class<U> stepClass) {
            final List<U> steps = TraversalHelper.getStepsOfAssignableClassRecursively(stepClass, traversal);
            return stepsAreParameterized(isParameterized, steps.toArray(new Step[steps.size()]));
        }

        /**
         * Verifies that a RangeContract is properly set up
         */
        public T isRangeGlobalGValueContract(final Step step, final long expectedLow, final long expectedHigh,
                                             final String lowName, final String highName) {
            assertThat("Step should be parameterized", step instanceof RangeGlobalStepPlaceholder && ((RangeGlobalStepPlaceholder<?>) step).isParameterized(), is(true));

            RangeGlobalStepPlaceholder<?> rangeGValueContract = (RangeGlobalStepPlaceholder<?>) step;

            assertEquals("Low range should match", expectedLow, rangeGValueContract.getLowRangeGValueSafe());
            assertEquals("High range should match", expectedHigh, rangeGValueContract.getHighRangeGValueSafe());
            assertEquals("Low range name should match", lowName, rangeGValueContract.getLowName());
            assertEquals("High range name should match", highName, rangeGValueContract.getHighName());

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
    }
}
