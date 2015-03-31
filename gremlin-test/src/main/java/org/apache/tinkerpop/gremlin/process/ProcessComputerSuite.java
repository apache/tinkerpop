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
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest;
import org.apache.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ExceptTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasNotTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RetainTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.BackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.TraversalVerificationStrategyTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@code ProcessComputerStandardSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} implementation.  This specialized test suite and runner is for use
 * by Gremlin implementers to test their {@link org.apache.tinkerpop.gremlin.structure.Graph} implementations.  The
 * {@code ProcessComputerStandardSuite} ensures consistency and validity of the implementations that they test.
 * <p/>
 * To use the {@code ProcessComputerStandardSuite} define a class in a test module.  Simple naming would expect the
 * name of the implementation followed by "ProcessComputerStandardSuite".  This class should be annotated as follows
 * (note that the "Suite" implements ProcessComputerStandardSuite.GraphProvider as a convenience only. It could be
 * implemented in a separate class file):
 * <code>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @RunWith(ProcessComputerSuite.class)
 * @ProcessComputerSuite.GraphProviderClass(TinkerGraphProcessComputerTest.class) public class TinkerGraphProcessComputerTest implements GraphProvider {
 * }
 * </code>
 * Implementing {@link org.apache.tinkerpop.gremlin.GraphProvider} provides a way for the {@code ProcessComputerStandardSuite}
 * to instantiate {@link org.apache.tinkerpop.gremlin.structure.Graph} instances from the implementation being tested to
 * inject into tests in the suite.  The {@code ProcessComputerStandardSuite} will utilized Features defined in the
 * suite to determine which tests will be executed.
 * <br/>
 */
public class ProcessComputerSuite extends AbstractGremlinSuite {

    // todo: all tests are not currently passing. see specific todos in each test

    /**
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[]{

            // computer, vertex program, and map/reduce semantics
            GraphComputerTest.class,

            // branch
            BranchTest.Traversals.class,
            ChooseTest.Traversals.class,
            LocalTest.Traversals.class,
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CoinTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            ExceptTest.StandardTest.class,
            FilterTest.Traversals.class,
            HasNotTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class,
            RetainTest.Traversals.class,
            SampleTest.Traversals.class,
            SimplePathTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            BackTest.Traversals.class,
            CoalesceTest.Traversals.class,
            CountTest.Traversals.class,
            FoldTest.Traversals.class,
            MapTest.Traversals.class,
            MaxTest.Traversals.class,
            MeanTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            // TODO: MatchTest.ComputerTest.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,
            VertexTest.Traversals.class,

            // sideEffect
            // TODO: AddEdgeTest.ComputerTest.class,
            AggregateTest.Traversals.class,
            GroupTest.Traversals.class,
            GroupCountTest.Traversals.class,
            // TODO: InjectTest.ComputerTest.class,
            ProfileTest.Traversals.class,
            SackTest.Traversals.class,
            SideEffectCapTest.Traversals.class,
            // TODO: REMOVE? SideEffectTest.ComputerTest.class,
            StoreTest.Traversals.class,
            // TODO: REMOVE? SubgraphTest.ComputerTest.class,
            TreeTest.Traversals.class,

            // algorithms
            PageRankVertexProgramTest.class,

            // strategy
            ComputerGraphTest.class,

            // strategy
            TraversalVerificationStrategyTest.ComputerTraversals.class,

            // decorations
            ReadOnlyStrategyProcessTest.class,
            SubgraphStrategyProcessTest.class
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
        super(klass, builder, testsToExecute, testsToExecute, false, TraversalEngine.Type.COMPUTER);
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, false, TraversalEngine.Type.COMPUTER);
    }

    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce, final boolean gremlinFlavorSuite) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, gremlinFlavorSuite, TraversalEngine.Type.COMPUTER);
    }

    @Override
    public boolean beforeTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        final UseEngine[] useEngines = testClass.getAnnotationsByType(UseEngine.class);
        if (null == useEngines || !Stream.of(useEngines).anyMatch(useEngine -> useEngine.value().equals(TraversalEngine.Type.COMPUTER)))
            throw new RuntimeException(String.format("The %s expects all tests to be annotated with @UseEngine(%s) - check %s",
                    ProcessComputerSuite.class.getName(), TraversalEngine.Type.COMPUTER, testClass.getName()));

        return super.beforeTestExecution(testClass);
    }
}
