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

using System;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.GremlinLangGeneration
{
    public class GremlinLangGenerationTests
    {
        [Fact]
        public void GraphTraversalStepsShouldUnrollParamsParameters()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            var gremlin = g.V().HasLabel("firstLabel", "secondLabel", "thirdLabel").GremlinLang.GetGremlin();

            Assert.Equal("g.V().hasLabel(\"firstLabel\",\"secondLabel\",\"thirdLabel\")", gremlin);
        }

        [Fact]
        public void g_V_OutXcreatedX()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            var gremlin = g.V().Out("created").GremlinLang.GetGremlin();

            Assert.Equal("g.V().out(\"created\")", gremlin);
        }

        [Fact]
        public void g_WithSackX1X_E_GroupCount_ByXweightX()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            var gremlin = g.WithSack(1).E().GroupCount<double>().By("weight").GremlinLang.GetGremlin();

            Assert.Equal("g.withSack(1).E().groupCount().by(\"weight\")", gremlin);
        }

        [Fact]
        public void g_InjectX1_2_3X()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            var gremlin = g.Inject(1, 2, 3).GremlinLang.GetGremlin();

            Assert.Equal("g.inject(1,2,3)", gremlin);
        }

        [Fact]
        public void AnonymousTraversal_Start_EmptyGremlinLang()
        {
            var gremlinLang = __.Start().GremlinLang;

            Assert.True(gremlinLang.IsEmpty);
        }

        [Fact]
        public void AnonymousTraversal_VXnullX()
        {
            var gremlin = __.V(null).GremlinLang.GetGremlin();

            Assert.Contains(".V(null)", gremlin);
        }
        
        [Fact]
        public void AnonymousTraversal_OutXnullX()
        {
            Assert.Throws<ArgumentNullException>(() => __.Out(null!));
        }
    }
}
