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

namespace Gremlin.Net.IntegrationTest.Process.Traversal.BytecodeGeneration
{
    public class BytecodeGenerationTests
    {
        [Fact]
        public void GraphTraversalStepsShouldUnrollParamsParameters()
        {
            var g = AnonymousTraversalSource.Traversal();

            var bytecode = g.V().HasLabel("firstLabel", "secondLabel", "thirdLabel").Bytecode;

            Assert.Empty(bytecode.SourceInstructions);
            Assert.Equal(2, bytecode.StepInstructions.Count);
            Assert.Equal(3, bytecode.StepInstructions[1].Arguments.Length);
        }

        [Fact]
        public void g_V_OutXcreatedX()
        {
            var g = AnonymousTraversalSource.Traversal();

            var bytecode = g.V().Out("created").Bytecode;

            Assert.Empty(bytecode.SourceInstructions);
            Assert.Equal(2, bytecode.StepInstructions.Count);
            Assert.Equal("V", bytecode.StepInstructions[0].OperatorName);
            Assert.Equal("out", bytecode.StepInstructions[1].OperatorName);
            Assert.Equal("created", bytecode.StepInstructions[1].Arguments[0]);
            Assert.Single(bytecode.StepInstructions[1].Arguments);
        }

        [Fact]
        public void g_WithSackX1X_E_GroupCount_ByXweightX()
        {
            var g = AnonymousTraversalSource.Traversal();

            var bytecode = g.WithSack(1).E().GroupCount<double>().By("weight").Bytecode;

            Assert.Single(bytecode.SourceInstructions);
            Assert.Equal("withSack", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(3, bytecode.StepInstructions.Count);
            Assert.Equal("E", bytecode.StepInstructions[0].OperatorName);
            Assert.Equal("groupCount", bytecode.StepInstructions[1].OperatorName);
            Assert.Equal("by", bytecode.StepInstructions[2].OperatorName);
            Assert.Equal("weight", bytecode.StepInstructions[2].Arguments[0]);
            Assert.Empty(bytecode.StepInstructions[0].Arguments);
            Assert.Empty(bytecode.StepInstructions[1].Arguments);
            Assert.Single(bytecode.StepInstructions[2].Arguments);
        }

        [Fact]
        public void g_InjectX1_2_3X()
        {
            var g = AnonymousTraversalSource.Traversal();

            var bytecode = g.Inject(1, 2, 3).Bytecode;

            Assert.Single(bytecode.StepInstructions);
            Assert.Equal("inject", bytecode.StepInstructions[0].OperatorName);
            Assert.Equal(3, bytecode.StepInstructions[0].Arguments.Length);
        }

        [Fact]
        public void AnonymousTraversal_Start_EmptyBytecode()
        {
            var bytecode = __.Start().Bytecode;

            Assert.Empty(bytecode.SourceInstructions);
            Assert.Empty(bytecode.StepInstructions);
        }

        [Fact]
        public void AnonymousTraversal_VXnullX()
        {
            var bytecode = __.V(null).Bytecode;
            
            Assert.Single(bytecode.StepInstructions);
            Assert.Single(bytecode.StepInstructions[0].Arguments);
            Assert.Null(bytecode.StepInstructions[0].Arguments[0]);
        }
        
        [Fact]
        public void AnonymousTraversal_OutXnullX()
        {
            Assert.Throws<ArgumentNullException>(() => __.Out(null));
        }
    }
}