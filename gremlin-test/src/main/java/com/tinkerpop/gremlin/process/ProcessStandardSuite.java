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
package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.TraversalSideEffectsTest;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.TraversalVerificationStrategyTest;
import com.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import com.tinkerpop.gremlin.process.util.PathTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code ProcessStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link com.tinkerpop.gremlin.structure.Graph} implementation.  This specialized test suite and runner is for use
 * by Gremlin implementers to test their {@link com.tinkerpop.gremlin.structure.Graph} implementations.  The
 * {@code ProcessStandardSuite} ensures consistency and validity of the implementations that they test.
 * <p/>
 * To use the {@code ProcessStandardSuite} define a class in a test module.  Simple naming would expect the name of the
 * implementation followed by "ProcessStandardSuite".  This class should be annotated as follows (note that the "Suite"
 * implements {@link com.tinkerpop.gremlin.GraphProvider} as a convenience only. It could be implemented in a
 * separate class file):
 * <p/>
 * <code>
 * @RunWith(ProcessStandardSuite.class)
 * @ProcessStandardSuite.GraphProviderClass(TinkerGraphProcessStandardTest.class) public class TinkerGraphProcessStandardTest implements GraphProvider {
 * }
 * </code>
 * <p/>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the {@code ProcessStandardSuite} to
 * instantiate {@link com.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to inject
 * into tests in the suite.  The ProcessStandardSuite will utilized
 * {@link com.tinkerpop.gremlin.structure.Graph.Features} defined in the suite to determine which tests will be executed.
 * <br/>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessStandardSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            BranchTest.StandardTest.class,
            ChooseTest.StandardTest.class,
            LocalTest.StandardTest.class,
            RepeatTest.StandardTest.class,
            UnionTest.StandardTest.class,

            // filter
            AndTest.StandardTest.class,
            CoinTest.StandardTest.class,
            CyclicPathTest.StandardTest.class,
            DedupTest.StandardTest.class,
            ExceptTest.StandardTest.class,
            FilterTest.StandardTest.class,
            HasNotTest.StandardTest.class,
            HasTest.StandardTest.class,
            IsTest.StandardTest.class,
            OrTest.StandardTest.class,
            RangeTest.StandardTest.class,
            RetainTest.StandardTest.class,
            SampleTest.StandardTest.class,
            SimplePathTest.StandardTest.class,
            WhereTest.StandardTest.class,

            // map
            BackTest.StandardTest.class,
            CountTest.StandardTest.class,
            FoldTest.StandardTest.class,
            MapTest.StandardTest.class,
            MatchTest.StandardTest.class,
            MaxTest.StandardTest.class,
            MeanTest.StandardTest.class,
            MinTest.StandardTest.class,
            OrderTest.StandardTest.class,
            com.tinkerpop.gremlin.process.graph.traversal.step.map.PathTest.StandardTest.class,
            PropertiesTest.StandardTest.class,
            SelectTest.StandardTest.class,
            VertexTest.StandardTest.class,
            UnfoldTest.StandardTest.class,
            ValueMapTest.StandardTest.class,
            CoalesceTest.StandardTest.class,

            // sideEffect
            AddEdgeTest.StandardTest.class,
            AggregateTest.StandardTest.class,
            GroupTest.StandardTest.class,
            GroupCountTest.StandardTest.class,
            InjectTest.StandardTest.class,
            ProfileTest.StandardTest.class,
            SackTest.StandardTest.class,
            SideEffectCapTest.StandardTest.class,
            SideEffectTest.StandardTest.class,
            StoreTest.StandardTest.class,
            SubgraphTest.StandardTest.class,
            TreeTest.StandardTest.class,

            // util
            TraversalSideEffectsTest.StandardTest.class,

            // compliance
            CoreTraversalTest.class,
            PathTest.class,

            // strategy
            TraversalVerificationStrategyTest.StandardTest.class

            // algorithms
            // PageRankVertexProgramTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static Class<?>[] testsToEnforce = new Class<?>[]{
            // branch
            BranchTest.class,
            ChooseTest.class,
            LocalTest.class,
            RepeatTest.class,
            UnionTest.class,

            // filter
            AndTest.class,
            CoinTest.class,
            CyclicPathTest.class,
            DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            RangeTest.class,
            RetainTest.class,
            SampleTest.class,
            SimplePathTest.class,
            WhereTest.class,

            // map
            BackTest.class,
            CountTest.class,
            FoldTest.class,
            MapTest.class,
            MatchTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            OrderTest.class,
            com.tinkerpop.gremlin.process.graph.traversal.step.map.PathTest.class,
            SelectTest.class,
            VertexTest.class,
            UnfoldTest.class,
            ValueMapTest.class,
            CoalesceTest.class,

            // sideEffect
            AddEdgeTest.class,
            AggregateTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            ProfileTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            SideEffectTest.class,
            StoreTest.class,
            SubgraphTest.class,
            TreeTest.class,

            // util
            TraversalSideEffectsTest.class,

            // compliance
            CoreTraversalTest.class,
            PathTest.class,

            // algorithms
            // PageRankVertexProgramTest.class

            // strategy
            TraversalVerificationStrategyTest.class
    };

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute;

    static {
        final String override = System.getenv().getOrDefault("gremlin.tests", "");
        if (override.equals(""))
            testsToExecute = allTests;
        else {
            final List<String> filters = Arrays.asList(override.split(","));
            final List<Class<?>> allowed = Stream.of(allTests)
                    .filter(c -> filters.contains(c.getName()))
                    .collect(Collectors.toList());
            testsToExecute = allowed.toArray(new Class<?>[allowed.size()]);
            testsToEnforce = testsToExecute;
        }
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessStandardSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce, final boolean gremlinFlavorSuite) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, gremlinFlavorSuite);
    }
}
