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
using Xunit;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class GraphTraversalSourceTests
    {
        [Fact]
        public void ShouldBeIndependentFromReturnedGraphTraversalModififyingBytecode()
        {
            var g = AnonymousTraversalSource.Traversal();

            g.V().Has("someKey", "someValue").Drop();

            Assert.Equal(0, g.Bytecode.StepInstructions.Count);
            Assert.Equal(0, g.Bytecode.SourceInstructions.Count);
        }

        [Fact]
        public void ShouldBeIndependentFromReturnedGraphTraversalSourceModififyingBytecode()
        {
            var g1 = AnonymousTraversalSource.Traversal();

            var g2 = g1.WithSideEffect("someSideEffectKey", "someSideEffectValue");

            Assert.Equal(0, g1.Bytecode.SourceInstructions.Count);
            Assert.Equal(0, g1.Bytecode.StepInstructions.Count);
            Assert.Equal(1, g2.Bytecode.SourceInstructions.Count);
        }

        [Fact]
        public void ShouldBeIndependentFromReturnedGraphTraversalSourceModififyingTraversalStrategies()
        {
            var gLocal = AnonymousTraversalSource.Traversal();

            var gRemote = gLocal.WithRemote(null);

            Assert.Equal(0, gLocal.TraversalStrategies.Count);
            Assert.Equal(1, gRemote.TraversalStrategies.Count);
        }
    }
}