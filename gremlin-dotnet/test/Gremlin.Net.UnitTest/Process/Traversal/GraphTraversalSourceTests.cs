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
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using NSubstitute;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class GraphTraversalSourceTests
    {
        [Fact]
        public void ShouldBeIndependentFromReturnedGraphTraversalModifyingBytecode()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            g.V().Has("someKey", "someValue").Drop();

            Assert.Empty(g.Bytecode.StepInstructions);
            Assert.Empty(g.Bytecode.SourceInstructions);
        }

        [Fact]
        public void ShouldBeIndependentFromReturnedGraphTraversalSourceModifyingBytecode()
        {
            var g1 = AnonymousTraversalSource.Traversal().With(null);

            var g2 = g1.WithSideEffect("someSideEffectKey", "someSideEffectValue");

            Assert.Empty(g1.Bytecode.SourceInstructions);
            Assert.Empty(g1.Bytecode.StepInstructions);
            Assert.Single(g2.Bytecode.SourceInstructions);
        }

        [Fact]
        public void CloneShouldCreateIndependentGraphTraversalModifyingBytecode()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);
            var original = g.V().Out("created");
            var clone = original.Clone().Out("knows");
            var cloneClone = clone.Clone().Out("created");
            
            Assert.Equal(2, original.Bytecode.StepInstructions.Count);
            Assert.Equal(3, clone.Bytecode.StepInstructions.Count);
            Assert.Equal(4, cloneClone.Bytecode.StepInstructions.Count);

            original.Has("person", "name", "marko");
            clone.V().Out();
            
            Assert.Equal(3, original.Bytecode.StepInstructions.Count);
            Assert.Equal(5, clone.Bytecode.StepInstructions.Count);
            Assert.Equal(4, cloneClone.Bytecode.StepInstructions.Count);
        }
        
        [Fact]
        public void ShouldOnlyAllowChildTraversalsThatAreAnonymous()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            g.V(0).AddE("self").To(__.V(1));

            Assert.Throws<ArgumentException>(() => g.V(0).AddE("self").To(g.V(1)));
        }
    }
}