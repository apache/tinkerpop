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

namespace Gremlin.Net.Process.UnitTest.Traversal
{
    public class TraverserTests
    {
        [Fact]
        public void ShouldReturnFalseForEqualsWhereOtherIsNull()
        {
            var traverser = new Traverser("anObject");

            var areEqual = traverser.Equals(null);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsWithSameObjectAndDifferentBulk()
        {
            var firstTraverser = new Traverser("anObject", 1234);
            var secondTraverser = new Traverser("anObject", 9876);

            var areEqual = firstTraverser.Equals(secondTraverser);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsObjectWithSameObjectAndDifferentBulk()
        {
            var firstTraverser = new Traverser("anObject", 1234);
            object secondTraverser = new Traverser("anObject", 9876);

            var areEqual = firstTraverser.Equals(secondTraverser);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnEqualHashcodesForTraversersWithSameObjectAndDifferentBulk()
        {
            var firstTraverser = new Traverser("anObject", 1234);
            var secondTraverser = new Traverser("anObject", 9876);

            var firstHashCode = firstTraverser.GetHashCode();
            var secondHashCode = secondTraverser.GetHashCode();

            Assert.Equal(firstHashCode, secondHashCode);
        }
    }
}