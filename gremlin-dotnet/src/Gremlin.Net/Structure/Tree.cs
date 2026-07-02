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
using System.Collections.Generic;
using System.Text;

namespace Gremlin.Net.Structure
{
    /// <summary>
    ///     A tree data structure with a tree-shaped public API.
    /// </summary>
    /// <remarks>
    ///     Iteration order over the nodes at a given level follows insertion order. Because children are keyed by node
    ///     value, sibling branches that resolve to the same value are represented as a single node; callers that require
    ///     every path to be preserved should use <see cref="Path" /> instead.
    ///     <para>
    ///         Children are stored in an ordered list of (key, child) entries rather than a dictionary so that
    ///         <c>null</c> keys are permitted and key comparisons use value-equality via <see cref="object.Equals(object)" />.
    ///     </para>
    /// </remarks>
    public class Tree : IEquatable<Tree>
    {
        private readonly List<KeyValuePair<object?, Tree>> _entries = new List<KeyValuePair<object?, Tree>>();

        /// <summary>
        ///     Initializes a new empty instance of the <see cref="Tree" /> class.
        /// </summary>
        public Tree()
        {
        }

        private int IndexOfKey(object? key)
        {
            for (var i = 0; i < _entries.Count; i++)
            {
                if (Equals(_entries[i].Key, key))
                {
                    return i;
                }
            }

            return -1;
        }

        // ------------------------------------------------------------------
        // navigation
        // ------------------------------------------------------------------

        /// <summary>
        ///     Returns the keys at the root of this tree in insertion order.
        /// </summary>
        public IReadOnlyList<object?> RootNodes()
        {
            var keys = new List<object?>(_entries.Count);
            foreach (var entry in _entries)
            {
                keys.Add(entry.Key);
            }

            return keys;
        }

        /// <summary>
        ///     Returns the child subtree for the given key.
        /// </summary>
        /// <param name="key">The key whose child subtree should be returned.</param>
        /// <returns>The child <see cref="Tree" /> for the given key.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if no child exists for the given key.</exception>
        public Tree ChildAt(object? key)
        {
            var index = IndexOfKey(key);
            if (index < 0)
            {
                throw new KeyNotFoundException($"Tree has no child for key: {key}");
            }

            return _entries[index].Value;
        }

        /// <summary>
        ///     Returns <c>true</c> if the given key is an immediate child of this tree.
        /// </summary>
        /// <param name="key">The key to look for.</param>
        public bool HasChild(object? key)
        {
            return IndexOfKey(key) >= 0;
        }

        /// <summary>
        ///     Returns <c>true</c> if the given value appears as a key anywhere in this tree (recursive).
        /// </summary>
        /// <param name="value">The value to search for.</param>
        public bool Contains(object? value)
        {
            foreach (var entry in _entries)
            {
                if (Equals(entry.Key, value))
                {
                    return true;
                }

                if (entry.Value.Contains(value))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        ///     Recursively searches the tree for the first subtree rooted at <paramref name="key" /> and returns it.
        ///     Direct children are visited before recursing.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <returns>The first matching subtree, or <c>null</c> if no match is found.</returns>
        public Tree? FindSubtree(object? key)
        {
            var index = IndexOfKey(key);
            if (index >= 0)
            {
                return _entries[index].Value;
            }

            foreach (var entry in _entries)
            {
                var found = entry.Value.FindSubtree(key);
                if (found != null)
                {
                    return found;
                }
            }

            return null;
        }

        /// <summary>
        ///     Returns the existing child for <paramref name="key" />, or inserts and returns a new empty
        ///     <see cref="Tree" /> if absent.
        /// </summary>
        /// <param name="key">The key whose child should be returned or created.</param>
        public Tree GetOrCreateChild(object? key)
        {
            var index = IndexOfKey(key);
            if (index >= 0)
            {
                return _entries[index].Value;
            }

            var child = new Tree();
            _entries.Add(new KeyValuePair<object?, Tree>(key, child));
            return child;
        }

        // ------------------------------------------------------------------
        // structural
        // ------------------------------------------------------------------

        /// <summary>
        ///     Returns <c>true</c> if this tree has no children. An empty tree is considered a leaf.
        /// </summary>
        public bool IsLeaf()
        {
            return _entries.Count == 0;
        }

        /// <summary>
        ///     Returns the total number of nodes (keys) in the tree, counted recursively.
        /// </summary>
        public int NodeCount()
        {
            var count = _entries.Count;
            foreach (var entry in _entries)
            {
                count += entry.Value.NodeCount();
            }

            return count;
        }

        /// <summary>
        ///     Returns the keys at the given depth. Depth <c>0</c> returns the root keys. Negative depths or depths
        ///     beyond the tree's height return an empty list.
        /// </summary>
        /// <param name="depth">The depth, where <c>0</c> is the root.</param>
        public IReadOnlyList<object?> GetNodesAtDepth(int depth)
        {
            var list = new List<object?>();
            foreach (var tree in GetTreesAtDepth(depth))
            {
                foreach (var entry in tree._entries)
                {
                    list.Add(entry.Key);
                }
            }

            return list;
        }

        /// <summary>
        ///     Returns the trees at the given depth. Depth <c>0</c> returns a singleton list containing this tree.
        ///     Depths beyond the tree's height (or negative depths) return an empty list.
        /// </summary>
        /// <param name="depth">The depth, where <c>0</c> is the root.</param>
        public IReadOnlyList<Tree> GetTreesAtDepth(int depth)
        {
            if (depth < 0)
            {
                return new List<Tree>(0);
            }

            var currentDepth = new List<Tree> { this };
            for (var i = 0; i < depth; i++)
            {
                var next = new List<Tree>();
                foreach (var tree in currentDepth)
                {
                    foreach (var entry in tree._entries)
                    {
                        next.Add(entry.Value);
                    }
                }

                if (next.Count == 0)
                {
                    return new List<Tree>(0);
                }

                currentDepth = next;
            }

            return currentDepth;
        }

        /// <summary>
        ///     Returns all keys whose subtrees are leaves.
        /// </summary>
        public IReadOnlyList<object?> GetLeafNodes()
        {
            var leaves = new List<object?>();
            CollectLeafKeys(leaves);
            return leaves;
        }

        private void CollectLeafKeys(List<object?> output)
        {
            foreach (var entry in _entries)
            {
                if (entry.Value.IsLeaf())
                {
                    output.Add(entry.Key);
                }
                else
                {
                    entry.Value.CollectLeafKeys(output);
                }
            }
        }

        /// <summary>
        ///     Returns single-key trees representing each leaf key in this tree, preserving the original
        ///     key-to-empty-subtree mapping.
        /// </summary>
        public IReadOnlyList<Tree> GetLeafTrees()
        {
            var leaves = new List<Tree>();
            CollectLeafTrees(leaves);
            return leaves;
        }

        private void CollectLeafTrees(List<Tree> output)
        {
            foreach (var entry in _entries)
            {
                if (entry.Value.IsLeaf())
                {
                    var leaf = new Tree();
                    leaf._entries.Add(new KeyValuePair<object?, Tree>(entry.Key, entry.Value));
                    output.Add(leaf);
                }
                else
                {
                    entry.Value.CollectLeafTrees(output);
                }
            }
        }

        // ------------------------------------------------------------------
        // composition
        // ------------------------------------------------------------------

        /// <summary>
        ///     Recursively merges <paramref name="other" /> into this tree. For overlapping keys, child subtrees are
        ///     merged in turn. For keys present only in <paramref name="other" />, the corresponding subtree is adopted
        ///     directly.
        /// </summary>
        /// <param name="other">The tree to merge into this one.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="other" /> is <c>null</c>.</exception>
        public void AddTree(Tree other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));

            foreach (var entry in other._entries)
            {
                var index = IndexOfKey(entry.Key);
                if (index >= 0)
                {
                    _entries[index].Value.AddTree(entry.Value);
                }
                else
                {
                    _entries.Add(new KeyValuePair<object?, Tree>(entry.Key, entry.Value));
                }
            }
        }

        /// <summary>
        ///     Splits this tree into one tree per root key. If the tree has a single root, returns a singleton list
        ///     containing this tree.
        /// </summary>
        public IReadOnlyList<Tree> SplitParents()
        {
            if (_entries.Count == 1)
            {
                return new List<Tree> { this };
            }

            var parents = new List<Tree>();
            foreach (var entry in _entries)
            {
                var parentTree = new Tree();
                parentTree._entries.Add(new KeyValuePair<object?, Tree>(entry.Key, entry.Value));
                parents.Add(parentTree);
            }

            return parents;
        }

        // ------------------------------------------------------------------
        // output
        // ------------------------------------------------------------------

        /// <summary>
        ///     Produces a formatted string representation of the tree structure using a <c>|--</c> ASCII style.
        ///     Each level is indented by three spaces relative to its parent, and there is no trailing newline.
        /// </summary>
        public string PrettyPrint()
        {
            const string lineSeparator = "\n";
            var builder = new StringBuilder();
            PrettyPrint(builder, "", lineSeparator);
            var pretty = builder.ToString();
            return pretty.Length == 0
                ? pretty
                : pretty.Substring(0, pretty.Length - lineSeparator.Length);
        }

        private void PrettyPrint(StringBuilder builder, string prefix, string lineSeparator)
        {
            foreach (var entry in _entries)
            {
                builder.Append(prefix).Append("|--").Append(entry.Key);
                builder.Append(lineSeparator);
                entry.Value.PrettyPrint(builder, prefix + "   ", lineSeparator);
            }
        }

        // ------------------------------------------------------------------
        // identity
        // ------------------------------------------------------------------

        /// <summary>
        ///     Structural recursive equality. Order across siblings is irrelevant; two trees are equal if they have the
        ///     same set of keys and each key's subtree is recursively equal.
        /// </summary>
        /// <param name="other">The other tree to compare against.</param>
        public bool Equals(Tree? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (_entries.Count != other._entries.Count) return false;

            foreach (var entry in _entries)
            {
                var index = other.IndexOfKey(entry.Key);
                if (index < 0) return false;
                if (!entry.Value.Equals(other._entries[index].Value)) return false;
            }

            return true;
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Tree) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            // Order-independent combination of root keys so that equal trees (which ignore sibling order) produce
            // equal hash codes. Subtrees are intentionally not folded in to keep this cheap while remaining
            // content-aware via the root keys.
            unchecked
            {
                var hashCode = 0;
                foreach (var entry in _entries)
                {
                    hashCode += entry.Key?.GetHashCode() ?? 0;
                }

                return hashCode;
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append('{');
            for (var i = 0; i < _entries.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.Append(_entries[i].Key).Append('=').Append(_entries[i].Value);
            }

            builder.Append('}');
            return builder.ToString();
        }
    }
}
