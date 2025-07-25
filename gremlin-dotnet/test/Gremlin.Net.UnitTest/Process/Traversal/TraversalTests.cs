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

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class TraversalTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData("test")]
        public void ShouldReturnAvailableTraverserObjWhenNextIsCalled(object traverserObj)
        {
            var traversal = new TestTraversal(new List<object?> {traverserObj});

            var actualObj = traversal.Next();

            Assert.Equal(traverserObj, actualObj);

            Assert.Null(traversal.Next());
        }
        [Theory]
        [InlineData(1)]
        [InlineData("test")]
        public void ShouldCheckHasNext(object traverserObj)
        {
            var traversal = new TestTraversal(new List<object?> {traverserObj});

            Assert.True(traversal.HasNext());
            Assert.True(traversal.HasNext());
            
            var actualObj = traversal.Next();
            Assert.Equal(traverserObj, actualObj);
            
            Assert.False(traversal.HasNext());
            Assert.False(traversal.HasNext());
        }

        [Theory]
        [InlineData(3)]
        [InlineData(10)]
        public void ShouldReturnCorrectNrOfResultObjsWhenNextIsCalledWithAmountArgument(int nrOfResults)
        {
            var objs = new List<object?>(20);
            for (var i = 0; i < 20; i++)
                objs.Add(i);
            var traversal = new TestTraversal(objs);

            var traversedObjs = traversal.Next(nrOfResults);

            var traversedObjsList = traversedObjs.ToList();
            Assert.Equal(nrOfResults, traversedObjsList.Count);
            for (var i = 0; i < nrOfResults; i++)
                Assert.Equal(objs[i], traversedObjsList[i]);
        }

        private List<object?> UnfoldBulks(IReadOnlyList<object?> objs, IReadOnlyList<long> bulks)
        {
            var unfoldedObjs = new List<object?>();
            for (var traverserIdx = 0; traverserIdx < objs.Count; traverserIdx++)
            for (var currentBulkObjIdx = 0; currentBulkObjIdx < bulks[traverserIdx]; currentBulkObjIdx++)
                unfoldedObjs.Add(objs[traverserIdx]);
            return unfoldedObjs;
        }

        [Fact]
        public void ShouldDrainAllTraversersWhenIterateIsCalled()
        {
            var someObjs = new List<object?> {1, 2, 3};
            var traversal = new TestTraversal(someObjs);

            var drainedTraversal = traversal.Iterate();

            Assert.Null(drainedTraversal.Next());
        }

        [Fact]
        public void ShouldReturnNullWhenNextIsCalledAndNoTraverserIsAvailable()
        {
            var expectedFirstObj = 1;
            var traversal = new TestTraversal(new List<object?> {expectedFirstObj});

            var actualFirstObj = traversal.Next();
            var actualSecondObj = traversal.Next();

            Assert.Equal(expectedFirstObj, actualFirstObj);
            Assert.Null(actualSecondObj);
        }

        [Fact]
        public void ShouldReturnTraversalsTraverserWhenNextTraverserIsCalled()
        {
            var someObjs = new List<object?> {1, 2, 3};
            var traversal = new TestTraversal(someObjs);

            var traverser = traversal.NextTraverser();

            Assert.Equal(traversal.Traversers!.First(), traverser);
        }

        [Fact]
        public void ShouldThrowNotSupportedExceptionWhenResetIsCalled()
        {
            var someObjs = new List<object?> {1, 2, 3};
            var traversal = new TestTraversal(someObjs);

            Assert.Throws<NotSupportedException>(() => traversal.Reset());
        }

        [Fact]
        public void ShouldReturnAllTraverserObjsWhenToListIsCalled()
        {
            var expectedObjs = new List<object?> {1, 2, 3};
            var traversal = new TestTraversal(expectedObjs);

            var traversedObjs = traversal.ToList();

            Assert.Equal(expectedObjs, traversedObjs);
        }

        [Fact]
        public void ShouldReturnAllTraverserObjWithoutDuplicatesWhenToSetIsCalled()
        {
            var traverserObjs = new List<object?> {1, 1, 2, 3};
            var traversal = new TestTraversal(traverserObjs);

            var traversedObjSet = traversal.ToSet();

            Assert.Equal(3, traversedObjSet.Count);
            Assert.Equal(new HashSet<object?>(traverserObjs), traversedObjSet);
        }

        [Fact]
        public void ShouldApplyStrategiesWhenNextIsCalledAndNoTraversersPresent()
        {
            const int expectedObj = 531;
            var testStrategy = new TestTraversalStrategy(new List<Traverser> {new Traverser(expectedObj)});
            var testTraversal = new TestTraversal(new List<ITraversalStrategy> {testStrategy});

            var actualObj = testTraversal.Next();

            Assert.Equal(expectedObj, actualObj);
        }

        [Fact]
        public void ShouldBeUnfoldTraverserBulksWhenToListIsCalled()
        {
            var objs = new List<object?> {1, 2, 3};
            var bulks = new List<long> {3, 2, 1};
            var traversal = new TestTraversal(objs, bulks);

            var traversedObjs = traversal.ToList();

            var expectedObjs = UnfoldBulks(objs, bulks);
            Assert.Equal(expectedObjs, traversedObjs);
        }

        [Fact]
        public void ShouldExtractIdFromVertex()
        {
            var g = AnonymousTraversalSource.Traversal().With(null);

            // Test basic V() step with mixed ID types
            var vStart = g.V(1, new Vertex(2));
            var vStartBytecode = vStart.Bytecode;
            Assert.Single(vStartBytecode.StepInstructions);
            Assert.Equal("V", vStartBytecode.StepInstructions[0].OperatorName);
            Assert.Equal(1, (int)vStartBytecode.StepInstructions[0].Arguments[0]);
            Assert.Equal(2, (int)vStartBytecode.StepInstructions[0].Arguments[1]); // ID should be extracted from Vertex

            // Test V() step in the middle of a traversal
            var vMid = g.Inject("foo").V(1, new Vertex(2));
            var vMidBytecode = vMid.Bytecode;
            Assert.Equal(2, vMidBytecode.StepInstructions.Count);
            Assert.Equal("inject", vMidBytecode.StepInstructions[0].OperatorName);
            Assert.Equal("foo", (string)vMidBytecode.StepInstructions[0].Arguments[0]);
            Assert.Equal("V", vMidBytecode.StepInstructions[1].OperatorName);
            Assert.Equal(1, (int)vMidBytecode.StepInstructions[1].Arguments[0]);
            Assert.Equal(2, (int)vMidBytecode.StepInstructions[1].Arguments[1]); // ID should be extracted from Vertex

            // Test edge creation with from/to vertices
            var fromTo = g.AddE("Edge").From(new Vertex(1)).To(new Vertex(2));
            var fromToBytecode = fromTo.Bytecode;
            Assert.Equal(3, fromToBytecode.StepInstructions.Count);
            Assert.Equal("addE", fromToBytecode.StepInstructions[0].OperatorName);
            Assert.Equal("Edge", (string)fromToBytecode.StepInstructions[0].Arguments[0]);
            Assert.Equal("from", fromToBytecode.StepInstructions[1].OperatorName);
            Assert.Equal(1, (int)fromToBytecode.StepInstructions[1].Arguments[0]); // ID should be extracted from Vertex
            Assert.Equal("to", fromToBytecode.StepInstructions[2].OperatorName);
            Assert.Equal(2, (int)fromToBytecode.StepInstructions[2].Arguments[0]); // ID should be extracted from Vertex

            // Test mergeE() with Vertex in dictionary
            var mergeMap = new Dictionary<object, object>
            {
                { T.Label, "knows" },
                { Direction.Out, new Vertex(1) },
                { Direction.In, new Vertex(2) }
            };

            var mergeEStart = g.MergeE(mergeMap);
            var mergeEStartBytecode = mergeEStart.Bytecode;
            Assert.Single(mergeEStartBytecode.StepInstructions);
            Assert.Equal("mergeE", mergeEStartBytecode.StepInstructions[0].OperatorName);
            
            // Check that the dictionary contains extracted IDs
            var mergeMapArg = (Dictionary<object, object>)mergeEStartBytecode.StepInstructions[0].Arguments[0];
            Assert.Equal("knows", (string)mergeMapArg[T.Label]);
            Assert.Equal(1, (int)mergeMapArg[Direction.Out]); // ID should be extracted from Vertex
            Assert.Equal(2, (int)mergeMapArg[Direction.In]); // ID should be extracted from Vertex

            // Test mergeE() in the middle of a traversal
            var mergeEMid = g.Inject("foo").MergeE(mergeMap);
            var mergeEMidBytecode = mergeEMid.Bytecode;
            Assert.Equal(2, mergeEMidBytecode.StepInstructions.Count);
            Assert.Equal("inject", mergeEMidBytecode.StepInstructions[0].OperatorName);
            Assert.Equal("foo", (string)mergeEMidBytecode.StepInstructions[0].Arguments[0]);
            Assert.Equal("mergeE", mergeEMidBytecode.StepInstructions[1].OperatorName);

            // Check that the dictionary contains extracted IDs
            var mergeMapArg2 = (Dictionary<object, object>)mergeEMidBytecode.StepInstructions[1].Arguments[0];
            Assert.Equal("knows", (string)mergeMapArg2[T.Label]);
            Assert.Equal(1, (int)mergeMapArg2[Direction.Out]); // ID should be extracted from Vertex
            Assert.Equal(2, (int)mergeMapArg2[Direction.In]); // ID should be extracted from Vertex
        }
    }
}