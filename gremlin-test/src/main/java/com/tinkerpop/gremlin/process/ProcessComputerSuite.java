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
import com.tinkerpop.gremlin.process.computer.GraphComputerTest;
import com.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import com.tinkerpop.gremlin.process.computer.util.ComputerDataStrategyTest;
import com.tinkerpop.gremlin.process.graph.traversal.step.branch.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.filter.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.*;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.*;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.TraversalVerificationStrategyTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code ProcessComputerStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link com.tinkerpop.gremlin.structure.Graph} implementation.  This specialized test suite and runner is for use
 * by Gremlin implementers to test their {@link com.tinkerpop.gremlin.structure.Graph} implementations.  The
 * {@code ProcessComputerStandardSuite} ensures consistency and validity of the implementations that they test.
 * <p/>
 * To use the {@code ProcessComputerStandardSuite} define a class in a test module.  Simple naming would expect the
 * name of the implementation followed by "ProcessComputerStandardSuite".  This class should be annotated as follows
 * (note that the "Suite" implements ProcessComputerStandardSuite.GraphProvider as a convenience only. It could be
 * implemented in a separate class file):
 * <code>
 * @RunWith(ProcessComputerSuite.class)
 * @ProcessComputerSuite.GraphProviderClass(TinkerGraphProcessComputerTest.class) public class TinkerGraphProcessComputerTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link com.tinkerpop.gremlin.GraphProvider} provides a way for the {@code ProcessComputerStandardSuite}
 * to instantiate {@link com.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to
 * inject into tests in the suite.  The {@code ProcessComputerStandardSuite} will utilized Features defined in the
 * suite to determine which tests will be executed.
 * <br/>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessComputerSuite extends AbstractGremlinSuite {

    // todo: all tests are not currently passing. see specific todos in each test

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{

            // basic api semantics testing
            GraphComputerTest.ComputerTest.class,   // todo: not sure this should be here as it forces retest of GraphComputer without an "implementation"

            // branch
            BranchTest.ComputerTest.class,
            ChooseTest.ComputerTest.class,
            LocalTest.ComputerTest.class,
            RepeatTest.ComputerTest.class,
            UnionTest.ComputerTest.class,

            // filter
            AndTest.ComputerTest.class,
            CoinTest.ComputerTest.class,
            CyclicPathTest.ComputerTest.class,
            // TODO: DedupTest.ComputerTest.class
            ExceptTest.ComputerTest.class,
            FilterTest.ComputerTest.class,
            HasNotTest.ComputerTest.class,
            HasTest.ComputerTest.class,
            IsTest.ComputerTest.class,
            OrTest.ComputerTest.class,
            // TODO: RangeTest.ComputerTest.class,
            RetainTest.ComputerTest.class,
            SampleTest.ComputerTest.class,
            SimplePathTest.ComputerTest.class,
            WhereTest.ComputerTest.class,

            // map
            BackTest.ComputerTest.class,
            CountTest.ComputerTest.class,
            FoldTest.ComputerTest.class,
            MapTest.ComputerTest.class,
            MaxTest.ComputerTest.class,
            MeanTest.ComputerTest.class,
            MinTest.ComputerTest.class,
            // TODO: MatchTest.ComputerTest.class,
            OrderTest.ComputerTest.class,
            PathTest.ComputerTest.class,
            PropertiesTest.ComputerTest.class,
            SelectTest.ComputerTest.class,
            UnfoldTest.ComputerTest.class,
            ValueMapTest.ComputerTest.class,
            VertexTest.ComputerTest.class,
            CoalesceTest.ComputerTest.class,

            // sideEffect
            // TODO: AddEdgeTest.ComputerTest.class,
            AggregateTest.ComputerTest.class,
            GroupTest.ComputerTest.class,
            GroupCountTest.ComputerTest.class,
            // TODO: InjectTest.ComputerTest.class,
            ProfileTest.ComputerTest.class,
            SackTest.ComputerTest.class,
            SideEffectCapTest.ComputerTest.class,
            // TODO: REMOVE? SideEffectTest.ComputerTest.class,
            StoreTest.ComputerTest.class,
            // TODO: REMOVE? SubgraphTest.ComputerTest.class,
            TreeTest.ComputerTest.class,

            // algorithms
            PageRankVertexProgramTest.class,

            // strategy
            ComputerDataStrategyTest.class,

            // strategy
            TraversalVerificationStrategyTest.ComputerTest.class
    };

    /**
     * Tests that will be enforced by the suite where instances of them should be in the list of testsToExecute.
     */
    protected static final Class<?>[] testsToEnforce = new Class<?>[]{
            // basic api semantics testing
            GraphComputerTest.class,

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
            // DedupTest.class,
            ExceptTest.class,
            FilterTest.class,
            HasNotTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            // RangeTest.class,
            RetainTest.class,
            SampleTest.class,
            SimplePathTest.class,
            WhereTest.class,


            // map
            BackTest.class,
            CountTest.class,
            // FoldTest.class,
            MapTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            // MatchTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            SelectTest.class,
            UnfoldTest.class,
            ValueMapTest.class,
            VertexTest.class,
            CoalesceTest.class,


            // sideEffect
            // AddEdgeTest.class,
            AggregateTest.class,
            GroupTest.class,
            GroupCountTest.class,
            InjectTest.class,
            ProfileTest.class,
            SackTest.class,
            SideEffectCapTest.class,
            // SideEffectTest.class,
            StoreTest.class,
            // SubgraphTest.class,
            TreeTest.class,


            // algorithms
            PageRankVertexProgramTest.class,

            // strategy
            ComputerDataStrategyTest.class,
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
        }
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce);
    }
}
