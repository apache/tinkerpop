#region License

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

using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Process.Traversal.Strategy.Finalization;
using Gremlin.Net.Process.Traversal.Strategy.Optimization;
using Gremlin.Net.Process.Traversal.Strategy.Verification;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.GremlinLangGeneration
{
    public class StrategiesTests
    {
        [Fact]
        public void TraversalWithoutStrategies_AfterWithStrategiesWasCalled_WithStrategiesNotAffected()
        {
            var g = new GraphTraversalSource().WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy());

            var gremlin = g.WithoutStrategies(typeof(ReadOnlyStrategy)).GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
            Assert.Contains("withoutStrategies(", gremlin);
        }

        [Fact]
        public void ShouldIncludeMultipleStrategiesInGremlinLangWhenGivenToWithoutStrategies()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithoutStrategies(typeof(ReadOnlyStrategy), typeof(LazyBarrierStrategy)).GremlinLang.GetGremlin();

            Assert.Contains("withoutStrategies(", gremlin);
            Assert.Contains("ReadOnlyStrategy", gremlin);
            Assert.Contains("LazyBarrierStrategy", gremlin);
        }

        [Fact]
        public void ShouldIncludeOneStrategyInGremlinLangWhenGivenToWithoutStrategies()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithoutStrategies(typeof(ReadOnlyStrategy)).GremlinLang.GetGremlin();

            Assert.Contains("withoutStrategies(", gremlin);
            Assert.Contains("ReadOnlyStrategy", gremlin);
        }

        [Fact]
        public void ShouldIncludeConfigurationInGremlinLangWhenGivenToWithStrategies()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithStrategies(new MatchAlgorithmStrategy("greedy")).GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
        }

        [Fact]
        public void ShouldIncludeMultipleStrategiesInGremlinLangWhenGivenToWithStrategies()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy()).GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
        }

        [Fact]
        public void ShouldIncludeOneStrategyInGremlinLangWhenGivenToWithStrategies()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithStrategies(new ReadOnlyStrategy()).GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
            Assert.Equal(0, g.TraversalStrategies.Count);
        }

        [Fact]
        public void TraversalWithStrategies_Strategies_ApplyToReusedGraphTraversalSource()
        {
            var g = new GraphTraversalSource().WithStrategies(new ReadOnlyStrategy(), new IncidentToAdjacentStrategy());

            var gremlin = g.V().GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
            Assert.Contains(".V()", gremlin);
        }

        [Fact]
        public void TraversalWithStrategies_StrategyWithTraversalInConfig_IncludeTraversalInGremlinLang()
        {
            var g = new GraphTraversalSource();

            var gremlin = g.WithStrategies(new SubgraphStrategy(__.Has("name", "marko"), checkAdjacentVertices: false)).GremlinLang.GetGremlin();

            Assert.Contains("withStrategies(", gremlin);
        }
    }
}
