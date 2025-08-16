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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier.AfterVerifier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier.BeforeVerifier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.GValueManagerVerifier.VerificationBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for the GValueManagerVerifier class.
 */
public class GValueManagerVerifierTest {

    /**
     * A simple strategy that doesn't modify the GValueManager state.
     */
    private static final class NoOpStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {
        private static final NoOpStrategy INSTANCE = new NoOpStrategy();

        private NoOpStrategy() {
        }

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // Do nothing
        }

        public static NoOpStrategy instance() {
            return INSTANCE;
        }
    }

    /**
     * Tests the verify method with a single strategy.
     */
    @Test
    public void testVerifyWithSingleStrategy() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the verify method returns a VerificationBuilder
        final VerificationBuilder<?, ?> builder = GValueManagerVerifier.verify(traversal, NoOpStrategy.instance());
        assertNotNull("VerificationBuilder should not be null", builder);
    }

    /**
     * Tests the verify method with multiple strategies.
     */
    @Test
    public void testVerifyWithMultipleStrategies() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Create a collection of additional strategies
        final Collection<TraversalStrategy> additionalStrategies = Collections.singletonList(NoOpStrategy.instance());

        // Verify that the verify method returns a VerificationBuilder
        final VerificationBuilder<?, ?> builder = GValueManagerVerifier.verify(traversal, NoOpStrategy.instance(), additionalStrategies);
        assertNotNull("VerificationBuilder should not be null", builder);
    }

    /**
     * Tests the beforeApplying method of VerificationBuilder.
     */
    @Test
    public void testBeforeApplying() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the beforeApplying method returns a BeforeVerifier
        final BeforeVerifier<?, ?> beforeVerifier = GValueManagerVerifier.verify(traversal, NoOpStrategy.instance()).beforeApplying();
        assertNotNull("BeforeVerifier should not be null", beforeVerifier);
    }

    /**
     * Tests the afterApplying method of VerificationBuilder.
     */
    @Test
    public void testAfterApplying() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the afterApplying method returns an AfterVerifier
        final AfterVerifier<?, ?> afterVerifier = GValueManagerVerifier.verify(traversal, NoOpStrategy.instance()).afterApplying();
        assertNotNull("AfterVerifier should not be null", afterVerifier);
    }

    /**
     * Tests the afterApplying method of BeforeVerifier.
     */
    @Test
    public void testBeforeVerifierAfterApplying() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the afterApplying method of BeforeVerifier returns an AfterVerifier
        final AfterVerifier<?, ?> afterVerifier = GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .afterApplying();
        assertNotNull("AfterVerifier should not be null", afterVerifier);
    }

    /**
     * Tests the variablesArePreserved method of AfterVerifier.
     */
    @Test
    public void testVariablesArePreserved() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that variables are preserved after applying the NoOpStrategy
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .afterApplying()
                .variablesArePreserved();
    }

    /**
     * Tests the notModified method of AfterVerifier.
     */
    @Test
    public void testNotModified() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the state is unchanged after applying the NoOpStrategy
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .stepsOfClassAreParameterized(true, HasStep.class)
                .afterApplying()
                .variablesArePreserved()
                .notModified();
    }

    /**
     * Tests the managerIsEmpty method of AbstractVerifier.
     */
    @Test
    public void testManagerIsEmpty() {
        // Create a traversal without GValues
        final Traversal.Admin<?, ?> traversal = __.out().asAdmin();

        // Verify that the manager is empty
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .managerIsEmpty();
    }

    /**
     * Tests the stepsAreParameterized method of AbstractVerifier.
     */
    @Test
    public void testStepsAreParameterized() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.has("age", GValue.of("x", 25)).asAdmin();

        // Get the HasStep from the traversal
        final HasStep<?> hasStep = TraversalHelper.getStepsOfClass(HasStep.class, traversal).get(0);

        // Verify that the HasStep is parameterized
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .stepsAreParameterized(true, hasStep);
    }

    /**
     * Tests the stepsOfClassAreParameterized method of AbstractVerifier.
     */
    @Test
    public void testStepsOfClassAreParameterized() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that all HasSteps are parameterized
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .stepsOfClassAreParameterized(true, HasStep.class);
    }

    /**
     * Tests the hasVariables method of AbstractVerifier with varargs.
     */
    @Test
    public void testHasVariablesVarargs() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Verify that the traversal has the expected variables
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .hasVariables("x");
    }

    /**
     * Tests the hasVariables method of AbstractVerifier with a Set.
     */
    @Test
    public void testHasVariablesSet() {
        // Create a traversal with GValues
        final Traversal.Admin<?, ?> traversal = __.filter(__.has("age", GValue.of("x", 25))).asAdmin();

        // Create a set of expected variables
        final Set<String> expectedVariables = new HashSet<>(Collections.singletonList("x"));

        // Verify that the traversal has the expected variables
        GValueManagerVerifier.verify(traversal, NoOpStrategy.instance())
                .beforeApplying()
                .hasVariables(expectedVariables);
    }
}
