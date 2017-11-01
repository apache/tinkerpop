﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections.Generic;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Process.Traversal.Strategy.Finalization;
using Gremlin.Net.Process.Traversal.Strategy.Optimization;
using Gremlin.Net.Process.Traversal.Strategy.Verification;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.BytecodeGeneration
{
    public class StrategiesTests
    {
        [Fact]
        public void TraversalWithoutStrategies_AfterWithStrategiesWasCalled_WithStrategiesNotAffected()
        {
            var graph = new Graph();
            var g = graph.Traversal().WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy());

            var bytecode = g.WithoutStrategies(typeof(ReadOnlyStrategy)).Bytecode;

            Assert.Equal(2, bytecode.SourceInstructions.Count);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(2, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal(new ReadOnlyStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(new IncidentToAdjacentStrategy(), bytecode.SourceInstructions[0].Arguments[1]);

            Assert.Equal("withoutStrategies", bytecode.SourceInstructions[1].OperatorName);
            Assert.Equal(1, bytecode.SourceInstructions[1].Arguments.Length);
            Assert.Equal(typeof(ReadOnlyStrategy), bytecode.SourceInstructions[1].Arguments[0]);
        }

        [Fact]
        public void ShouldIncludeMultipleStrategiesInBytecodeWhenGivenToWithoutStrategies()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithoutStrategies(typeof(ReadOnlyStrategy), typeof(LazyBarrierStrategy)).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(2, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withoutStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(typeof(ReadOnlyStrategy), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(typeof(LazyBarrierStrategy), bytecode.SourceInstructions[0].Arguments[1]);
        }

        [Fact]
        public void ShouldIncludeOneStrategyInBytecodeWhenGivenToWithoutStrategies()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithoutStrategies(typeof(ReadOnlyStrategy)).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withoutStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(typeof(ReadOnlyStrategy), bytecode.SourceInstructions[0].Arguments[0]);
        }

        [Fact]
        public void ShouldIncludeConfigurationInBytecodeWhenGivenToWithStrategies()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithStrategies(new MatchAlgorithmStrategy("greedy")).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(new MatchAlgorithmStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Contains("greedy",
                ((MatchAlgorithmStrategy) bytecode.SourceInstructions[0].Arguments[0]).Configuration.Values);
        }

        [Fact]
        public void ShouldIncludeMultipleStrategiesInBytecodeWhenGivenToWithStrategies()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy()).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(2, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(new ReadOnlyStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(new IncidentToAdjacentStrategy(), bytecode.SourceInstructions[0].Arguments[1]);
        }

        [Fact]
        public void ShouldIncludeOneStrategyInBytecodeWhenGivenToWithStrategies()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithStrategies(new ReadOnlyStrategy()).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(new ReadOnlyStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal("ReadOnlyStrategy", bytecode.SourceInstructions[0].Arguments[0].ToString());
            Assert.Equal(new ReadOnlyStrategy().GetHashCode(), bytecode.SourceInstructions[0].Arguments[0].GetHashCode());
            Assert.Equal(0, g.TraversalStrategies.Count);
        }

        [Fact]
        public void TraversalWithStrategies_Strategies_ApplyToReusedGraphTraversalSource()
        {
            var graph = new Graph();
            var g = graph.Traversal().WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy());

            var bytecode = g.V().Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(2, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(new ReadOnlyStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(new IncidentToAdjacentStrategy(), bytecode.SourceInstructions[0].Arguments[1]);
            Assert.Equal(1, bytecode.StepInstructions.Count);
            Assert.Equal("V", bytecode.StepInstructions[0].OperatorName);
        }

        [Fact]
        public void TraversalWithStrategies_StrategyWithTraversalInConfig_IncludeTraversalInInConfigInBytecode()
        {
            var graph = new Graph();
            var g = graph.Traversal();

            var bytecode = g.WithStrategies(new SubgraphStrategy(__.Has("name", "marko"))).Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments.Length);
            Assert.Equal("withStrategies", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(new SubgraphStrategy(), bytecode.SourceInstructions[0].Arguments[0]);
            SubgraphStrategy strategy = bytecode.SourceInstructions[0].Arguments[0];
            Assert.Equal(1, strategy.Configuration.Count);
            Assert.Equal(typeof(GraphTraversal<object, object>), strategy.Configuration["vertices"].GetType());
            ITraversal traversal = strategy.Configuration["vertices"];
            Assert.Equal("has", traversal.Bytecode.StepInstructions[0].OperatorName);
            Assert.Equal(new List<string> {"name", "marko"}, traversal.Bytecode.StepInstructions[0].Arguments);
        }
    }
}