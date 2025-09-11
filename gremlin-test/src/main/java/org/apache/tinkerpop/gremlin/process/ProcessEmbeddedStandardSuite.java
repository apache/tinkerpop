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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparabilitySemanticsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ExplainTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategyProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code ProcessEmbeddedStandardSuite} is a set of tests that providers may implement in addition to the standard
 * Gherkin feature tests. These tests are only relevant for embedded use cases covering features like lambdas or other
 * capabilities that can only work when running Gremlin in the same JVM as the {@link Graph} instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessEmbeddedStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed as part of this suite.
     */
    private static final Class<?>[] allTests = new Class<?>[]{

            LambdaStepTest.Traversals.class,
            MatchTest.CountMatchTraversals.class,
            MatchTest.GreedyMatchTraversals.class,
            ProfileTest.Traversals.class,
            WriteTest.Traversals.class,
            ExplainTest.Traversals.class,

            // compliance
            CoreTraversalTest.class,
            TraversalInterruptionTest.class,

            // creations
            TranslationStrategyProcessTest.class,

            // decorations
            ElementIdStrategyProcessTest.class,
            EventStrategyProcessTest.class,

            // optimizations
            IncidentToAdjacentStrategyProcessTest.class,
            EarlyLimitStrategyProcessTest.class,

            // semantics
            ComparabilitySemanticsTest.class,
    };

    /**
     * A list of the minimum set of base tests that Gremlin flavors should implement to be compliant with Gremlin.
     */
    private static final Class<?>[] testsToEnforce = new Class<?>[]{
            ProfileTest.class,
    };

    /**
     * This constructor is used by JUnit and will run this suite with its concrete implementations of the
     * {@code testsToEnforce}.
     */
    public ProcessEmbeddedStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }

    /**
     * This constructor is used by Gremlin flavor implementers who supply their own implementations of the
     * {@code testsToEnforce}.
     */
    public ProcessEmbeddedStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.STANDARD);
    }
}