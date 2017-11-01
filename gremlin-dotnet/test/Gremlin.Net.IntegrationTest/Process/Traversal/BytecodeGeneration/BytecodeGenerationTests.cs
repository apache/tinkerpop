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
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.BytecodeGeneration
{
    public class BytecodeGenerationTests
    {
        [Fact]
        public void GraphTraversalStepsShouldUnrollParamsParameters()
        {
            var g = new Graph().Traversal();

            var bytecode = g.V().HasLabel("firstLabel", "secondLabel", "thirdLabel").Bytecode;

            Assert.Equal(0, bytecode.SourceInstructions.Count);
            Assert.Equal(2, bytecode.StepInstructions.Count);
            Assert.Equal(3, bytecode.StepInstructions[1].Arguments.Length);
        }

        [Fact]
        public void g_V_OutXcreatedX()
        {
            var g = new Graph().Traversal();

            var bytecode = g.V().Out("created").Bytecode;

            Assert.Equal(0, bytecode.SourceInstructions.Count);
            Assert.Equal(2, bytecode.StepInstructions.Count);
            Assert.Equal("V", bytecode.StepInstructions[0].OperatorName);
            Assert.Equal("out", bytecode.StepInstructions[1].OperatorName);
            Assert.Equal("created", bytecode.StepInstructions[1].Arguments[0]);
            Assert.Equal(1, bytecode.StepInstructions[1].Arguments.Length);
        }

        [Fact]
        public void g_WithSackX1X_E_GroupCount_ByXweightX()
        {
            var g = new Graph().Traversal();

            var bytecode = g.WithSack(1).E().GroupCount<double>().By("weight").Bytecode;

            Assert.Equal(1, bytecode.SourceInstructions.Count);
            Assert.Equal("withSack", bytecode.SourceInstructions[0].OperatorName);
            Assert.Equal(1, bytecode.SourceInstructions[0].Arguments[0]);
            Assert.Equal(3, bytecode.StepInstructions.Count);
            Assert.Equal("E", bytecode.StepInstructions[0].OperatorName);
            Assert.Equal("groupCount", bytecode.StepInstructions[1].OperatorName);
            Assert.Equal("by", bytecode.StepInstructions[2].OperatorName);
            Assert.Equal("weight", bytecode.StepInstructions[2].Arguments[0]);
            Assert.Equal(0, bytecode.StepInstructions[0].Arguments.Length);
            Assert.Equal(0, bytecode.StepInstructions[1].Arguments.Length);
            Assert.Equal(1, bytecode.StepInstructions[2].Arguments.Length);
        }

        [Fact]
        public void AnonymousTraversal_Start_EmptyBytecode()
        {
            var bytecode = __.Start().Bytecode;

            Assert.Equal(0, bytecode.SourceInstructions.Count);
            Assert.Equal(0, bytecode.StepInstructions.Count);
        }
    }
}