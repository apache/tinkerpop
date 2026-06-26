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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using System.Linq;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class TreeTests
    {
        // Builds: marko -> {lop, josh -> {ripple, lop}}
        private static Tree BuildSampleTree()
        {
            var tree = new Tree();
            var marko = tree.GetOrCreateChild("marko");
            marko.GetOrCreateChild("lop");
            var josh = marko.GetOrCreateChild("josh");
            josh.GetOrCreateChild("ripple");
            josh.GetOrCreateChild("lop");
            return tree;
        }

        [Fact]
        public void RootNodesShouldReturnKeysInInsertionOrder()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("a");
            tree.GetOrCreateChild("b");
            tree.GetOrCreateChild("c");

            Assert.Equal(new object?[] { "a", "b", "c" }, tree.RootNodes());
        }

        [Fact]
        public void NodeCountShouldCountAllKeysRecursively()
        {
            // marko, lop, josh, ripple, lop => 5 keys total
            Assert.Equal(5, BuildSampleTree().NodeCount());
            Assert.Equal(0, new Tree().NodeCount());
        }

        [Fact]
        public void GetNodesAtDepthShouldReturnKeysForEachLevel()
        {
            var tree = BuildSampleTree();

            Assert.Equal(new object?[] { "marko" }, tree.GetNodesAtDepth(0));

            var depth1 = tree.GetNodesAtDepth(1);
            Assert.Equal(new object?[] { "lop", "josh" }, depth1);

            var depth2 = tree.GetNodesAtDepth(2);
            Assert.Equal(new object?[] { "ripple", "lop" }, depth2);
        }

        [Fact]
        public void GetNodesAtDepthShouldReturnEmptyForNegativeOrOutOfRangeDepth()
        {
            var tree = BuildSampleTree();

            Assert.Empty(tree.GetNodesAtDepth(-1));
            Assert.Empty(tree.GetNodesAtDepth(3));
            Assert.Empty(tree.GetNodesAtDepth(99));
        }

        [Fact]
        public void GetTreesAtDepthShouldReturnSubtreesForEachLevel()
        {
            var tree = BuildSampleTree();

            var depth0 = tree.GetTreesAtDepth(0);
            Assert.Single(depth0);
            Assert.Same(tree, depth0[0]);

            // depth 1 holds the marko subtree
            var depth1 = tree.GetTreesAtDepth(1);
            Assert.Single(depth1);
            Assert.True(depth1[0].HasChild("lop"));
            Assert.True(depth1[0].HasChild("josh"));

            // depth 2 holds the lop (leaf) and josh subtrees
            var depth2 = tree.GetTreesAtDepth(2);
            Assert.Equal(2, depth2.Count);

            Assert.Empty(tree.GetTreesAtDepth(-1));
            Assert.Empty(tree.GetTreesAtDepth(5));
        }

        [Fact]
        public void GetLeafNodesShouldReturnKeysWithEmptySubtrees()
        {
            var tree = BuildSampleTree();

            // leaves: lop (under marko), ripple and lop (under josh)
            Assert.Equal(new object?[] { "lop", "ripple", "lop" }, tree.GetLeafNodes());
        }

        [Fact]
        public void GetLeafTreesShouldReturnSingleKeyTreesForEachLeaf()
        {
            var tree = BuildSampleTree();

            var leafTrees = tree.GetLeafTrees();
            Assert.Equal(3, leafTrees.Count);
            foreach (var leafTree in leafTrees)
            {
                Assert.Single(leafTree.RootNodes());
            }

            Assert.Equal(new object?[] { "lop", "ripple", "lop" },
                leafTrees.SelectMany(t => t.RootNodes()).ToArray());
        }

        [Fact]
        public void SplitParentsShouldReturnOneTreePerRootKey()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("a").GetOrCreateChild("a1");
            tree.GetOrCreateChild("b");

            var parents = tree.SplitParents();
            Assert.Equal(2, parents.Count);
            Assert.True(parents[0].HasChild("a"));
            Assert.True(parents[1].HasChild("b"));
            Assert.True(parents[0].ChildAt("a").HasChild("a1"));
        }

        [Fact]
        public void SplitParentsShouldReturnSelfForSingleRoot()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("only");

            var parents = tree.SplitParents();
            Assert.Single(parents);
            Assert.Same(tree, parents[0]);
        }

        [Fact]
        public void AddTreeShouldMergeOverlappingKeysRecursively()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("marko").GetOrCreateChild("lop");

            var other = new Tree();
            var otherMarko = other.GetOrCreateChild("marko");
            otherMarko.GetOrCreateChild("josh");
            other.GetOrCreateChild("vadas");

            tree.AddTree(other);

            // marko branch merged, vadas branch adopted
            Assert.Equal(new object?[] { "marko", "vadas" }, tree.RootNodes());
            var merged = tree.ChildAt("marko");
            Assert.True(merged.HasChild("lop"));
            Assert.True(merged.HasChild("josh"));
        }

        [Fact]
        public void AddTreeShouldThrowOnNull()
        {
            var tree = new Tree();
            Assert.Throws<ArgumentNullException>(() => tree.AddTree(null!));
        }

        [Fact]
        public void ContainsShouldFindValuesRecursively()
        {
            var tree = BuildSampleTree();

            Assert.True(tree.Contains("marko"));
            Assert.True(tree.Contains("ripple"));
            Assert.True(tree.Contains("lop"));
            Assert.False(tree.Contains("peter"));
        }

        [Fact]
        public void FindSubtreeShouldReturnFirstMatchingSubtree()
        {
            var tree = BuildSampleTree();

            var joshSubtree = tree.FindSubtree("josh");
            Assert.NotNull(joshSubtree);
            Assert.True(joshSubtree!.HasChild("ripple"));
            Assert.True(joshSubtree.HasChild("lop"));

            // a leaf key resolves to an empty subtree
            var rippleSubtree = tree.FindSubtree("ripple");
            Assert.NotNull(rippleSubtree);
            Assert.True(rippleSubtree!.IsLeaf());

            Assert.Null(tree.FindSubtree("absent"));
        }

        [Fact]
        public void EqualsShouldBeOrderInsensitiveAcrossSiblings()
        {
            var t1 = new Tree();
            t1.GetOrCreateChild("a").GetOrCreateChild("b");
            t1.GetOrCreateChild("c");

            var t2 = new Tree();
            // reversed sibling insertion order
            t2.GetOrCreateChild("c");
            t2.GetOrCreateChild("a").GetOrCreateChild("b");

            Assert.True(t1.Equals(t2));
            Assert.True(t2.Equals(t1));
            Assert.Equal(t1, t2);
        }

        [Fact]
        public void EqualsShouldReturnFalseForNull()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("a");

            Assert.False(tree.Equals(null));
            Assert.False(tree.Equals((object?) null));
        }

        [Fact]
        public void EqualsShouldReturnTrueForSameReference()
        {
            var tree = BuildSampleTree();

            Assert.True(tree.Equals(tree));
            Assert.True(tree.Equals((object) tree));
        }

        [Fact]
        public void EqualsShouldReturnFalseForDifferentEntryCount()
        {
            var t1 = new Tree();
            t1.GetOrCreateChild("a");
            t1.GetOrCreateChild("b");

            var t2 = new Tree();
            t2.GetOrCreateChild("a");

            Assert.False(t1.Equals(t2));
        }

        [Fact]
        public void EqualsShouldReturnFalseForDifferingSubtree()
        {
            var t1 = new Tree();
            t1.GetOrCreateChild("a").GetOrCreateChild("b");

            var t2 = new Tree();
            t2.GetOrCreateChild("a").GetOrCreateChild("x");

            Assert.False(t1.Equals(t2));
        }

        [Fact]
        public void EqualsObjectOverrideShouldReturnFalseForOtherTypes()
        {
            var tree = new Tree();
            tree.GetOrCreateChild("a");

            Assert.False(tree.Equals("not a tree"));
            Assert.False(tree.Equals(42));
        }

        [Fact]
        public void EmptyTreesShouldBeEqual()
        {
            Assert.True(new Tree().Equals(new Tree()));
            Assert.Equal(new Tree(), new Tree());
        }

        [Fact]
        public void GetHashCodeShouldBeEqualForEqualTrees()
        {
            var t1 = new Tree();
            t1.GetOrCreateChild("a").GetOrCreateChild("b");
            t1.GetOrCreateChild("c");

            var t2 = new Tree();
            // reversed sibling order, structurally equal
            t2.GetOrCreateChild("c");
            t2.GetOrCreateChild("a").GetOrCreateChild("b");

            Assert.True(t1.Equals(t2));
            Assert.Equal(t1.GetHashCode(), t2.GetHashCode());
        }

        [Fact]
        public void GetHashCodeShouldBeStableAcrossCalls()
        {
            var tree = BuildSampleTree();

            Assert.Equal(tree.GetHashCode(), tree.GetHashCode());
        }

        [Fact]
        public void GetHashCodeShouldHandleNullKey()
        {
            // GetHashCode folds in entry.Key?.GetHashCode() ?? 0, so a null key
            // must not throw.
            var tree = new Tree();
            tree.GetOrCreateChild(null).GetOrCreateChild("child");

            var hash = tree.GetHashCode();

            var other = new Tree();
            other.GetOrCreateChild(null).GetOrCreateChild("child");
            Assert.Equal(hash, other.GetHashCode());
        }
    }
}
