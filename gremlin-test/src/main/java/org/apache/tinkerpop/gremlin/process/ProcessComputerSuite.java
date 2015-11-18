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
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.computer.ranking.PageRankVertexProgramTest;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapKeysTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapValuesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ExplainTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTestV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * The {@code ProcessComputerSuite} is a JUnit test runner that executes the Gremlin Test Suite over a
 * {@link Graph} implementation.  This test suite covers traversal operations around {@link GraphComputer} and should
 * be implemented by vendors to validate that their implementations are compliant with that Gremlin language.
 * Implementations that use this test suite should return {@code true} for
 * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#supportsComputer()}.
 * <p/>
 * For more information on the usage of this suite, please see {@link StructureStandardSuite}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ProcessComputerSuite extends AbstractGremlinSuite {

    /**
     * This list of tests in the suite that will be executed as part of this suite.
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
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class,
            SampleTest.Traversals.class,
            SimplePathTest.Traversals.class,
            TailTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            CoalesceTest.Traversals.class,
            ConstantTest.Traversals.class,
            CountTest.Traversals.class,
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            GraphTest.Traversals.class,
            LoopsTest.Traversals.class,
            MapTest.Traversals.class,
            MapKeysTest.Traversals.class,
            MapValuesTest.Traversals.class,
            MatchTest.CountMatchTraversals.class,
            MatchTest.GreedyMatchTraversals.class,
            MaxTest.Traversals.class,
            MeanTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,
            VertexTest.Traversals.class,

            // sideEffect
            AddEdgeTest.Traversals.class,
            AggregateTest.Traversals.class,
            ExplainTest.Traversals.class,
            GroupTest.Traversals.class,
            GroupTestV3d0.Traversals.class,
            GroupCountTest.Traversals.class,
            InjectTest.Traversals.class,
            ProfileTest.Traversals.class,
            SackTest.Traversals.class,
            SideEffectCapTest.Traversals.class,
            SideEffectTest.Traversals.class,
            StoreTest.Traversals.class,
            SubgraphTest.Traversals.class,
            TreeTest.Traversals.class,

            // algorithms
            PageRankVertexProgramTest.class,
            BulkLoaderVertexProgramTest.class,

            // strategy
            ComputerVerificationStrategyProcessTest.ComputerTraversals.class,

            // decorations
            ReadOnlyStrategyProcessTest.class,
            SubgraphStrategyProcessTest.class
    };

    /**
     * A list of the minimum set of base tests that Gremlin flavors should implement to be compliant with Gremlin.
     */
    private static final Class<?>[] testsToEnforce = new Class<?>[]{
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
            FilterTest.class,
            HasTest.class,
            IsTest.class,
            OrTest.class,
            RangeTest.class,
            SampleTest.class,
            SimplePathTest.class,
            TailTest.class,
            WhereTest.class,

            // map
            CoalesceTest.class,
            ConstantTest.class,
            CountTest.class,
            FlatMapTest.class,
            FoldTest.class,
            MapTest.class,
            MaxTest.class,
            MeanTest.class,
            MinTest.class,
            SumTest.class,
            MatchTest.class,
            OrderTest.class,
            PathTest.class,
            PropertiesTest.class,
            SelectTest.class,
            UnfoldTest.class,
            ValueMapTest.class,
            VertexTest.class,

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
            TreeTest.class
    };

    /**
     * This constructor is used by JUnit and will run this suite with its concrete implementations of the
     * {@code testsToEnforce}.
     */
    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, allTests, false, TraversalEngine.Type.COMPUTER);
    }

    /**
     * This constructor is used by Gremlin flavor implementers who supply their own implementations of the
     * {@code testsToEnforce}.
     */
    public ProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, testsToExecute, testsToEnforce, true, TraversalEngine.Type.COMPUTER);
    }
}
