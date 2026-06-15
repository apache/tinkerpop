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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A tree data structure with a tree-shaped public API.
 * <p>
 * Iteration order over the nodes at a given level is not promised. Because children are keyed by node value,
 * sibling branches that resolve to the same value are represented as a single node; callers that require every
 * path to be preserved should use {@link org.apache.tinkerpop.gremlin.process.traversal.Path} instead.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Tree<T> implements Serializable {

    private final Map<T, Tree<T>> children = new HashMap<>();

    public Tree() {
    }

    /**
     * Creates a tree with the given values as root keys, each mapping to an empty {@link Tree}.
     */
    @SafeVarargs
    public Tree(final T... rootValues) {
        for (final T t : rootValues) {
            this.children.put(t, new Tree<>());
        }
    }

    // ------------------------------------------------------------------
    // navigation
    // ------------------------------------------------------------------

    /**
     * Returns the set of keys at the root of this tree. The returned set is unmodifiable.
     */
    public Set<T> rootNodes() {
        return Collections.unmodifiableSet(this.children.keySet());
    }

    /**
     * Returns the child subtree for the given key.
     *
     * @throws IllegalArgumentException if no child exists for the given key
     */
    public Tree<T> childAt(final T key) {
        if (!this.children.containsKey(key)) {
            throw new IllegalArgumentException("Tree has no child for key: " + key);
        }
        return this.children.get(key);
    }

    /**
     * Returns {@code true} if the given key is an immediate child of this tree.
     */
    public boolean hasChild(final T key) {
        return this.children.containsKey(key);
    }

    /**
     * Returns {@code true} if the given value appears as a key anywhere in this tree (recursive).
     */
    public boolean contains(final T value) {
        if (this.children.containsKey(value)) {
            return true;
        }
        for (final Tree<T> sub : this.children.values()) {
            if (sub.contains(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Recursively searches the tree for the first subtree rooted at {@code key} and returns it. The search visits
     * direct children first and then recurses, but iteration order across siblings is unspecified.
     */
    public Optional<Tree<T>> findSubtree(final T key) {
        if (this.children.containsKey(key)) {
            return Optional.of(this.children.get(key));
        }
        for (final Tree<T> sub : this.children.values()) {
            final Optional<Tree<T>> found = sub.findSubtree(key);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the existing child for {@code key}, or inserts and returns a new empty {@link Tree} if absent.
     */
    public Tree<T> getOrCreateChild(final T key) {
        Tree<T> child = this.children.get(key);
        if (null == child) {
            child = new Tree<>();
            this.children.put(key, child);
        }
        return child;
    }

    // ------------------------------------------------------------------
    // structural
    // ------------------------------------------------------------------

    /**
     * Returns {@code true} if this tree has no children. An empty tree is considered a leaf.
     */
    public boolean isLeaf() {
        return this.children.isEmpty();
    }

    /**
     * Returns the total number of nodes (keys) in the tree, counted recursively.
     */
    public int nodeCount() {
        int count = this.children.size();
        for (final Tree<T> sub : this.children.values()) {
            count += sub.nodeCount();
        }
        return count;
    }

    /**
     * Returns the keys at the given depth. Depth {@code 0} returns the root keys.
     */
    public List<T> getNodesAtDepth(final int depth) {
        final List<T> list = new ArrayList<>();
        for (final Tree<T> t : this.getTreesAtDepth(depth)) {
            list.addAll(t.children.keySet());
        }
        return list;
    }

    /**
     * Returns the trees at the given depth. Depth {@code 0} returns a singleton list containing this tree.
     * Depths beyond the tree's height return an empty list.
     */
    public List<Tree<T>> getTreesAtDepth(final int depth) {
        if (depth < 0) {
            return Collections.emptyList();
        }
        List<Tree<T>> currentDepth = Collections.singletonList(this);
        for (int i = 0; i < depth; i++) {
            final List<Tree<T>> next = new ArrayList<>();
            for (final Tree<T> t : currentDepth) {
                next.addAll(t.children.values());
            }
            if (next.isEmpty()) {
                return Collections.emptyList();
            }
            currentDepth = next;
        }
        return currentDepth;
    }

    /**
     * Returns all keys whose subtrees are leaves.
     */
    public List<T> getLeafNodes() {
        final List<T> leaves = new ArrayList<>();
        collectLeafKeys(leaves);
        return leaves;
    }

    private void collectLeafKeys(final List<T> out) {
        for (final Map.Entry<T, Tree<T>> entry : this.children.entrySet()) {
            if (entry.getValue().isLeaf()) {
                out.add(entry.getKey());
            } else {
                entry.getValue().collectLeafKeys(out);
            }
        }
    }

    /**
     * Returns single-key trees representing each leaf key in this tree, preserving the original
     * key-to-empty-subtree mapping.
     */
    public List<Tree<T>> getLeafTrees() {
        final List<Tree<T>> leaves = new ArrayList<>();
        collectLeafTrees(leaves);
        return leaves;
    }

    private void collectLeafTrees(final List<Tree<T>> out) {
        for (final Map.Entry<T, Tree<T>> entry : this.children.entrySet()) {
            if (entry.getValue().isLeaf()) {
                final Tree<T> leaf = new Tree<>();
                leaf.children.put(entry.getKey(), entry.getValue());
                out.add(leaf);
            } else {
                entry.getValue().collectLeafTrees(out);
            }
        }
    }

    // ------------------------------------------------------------------
    // composition
    // ------------------------------------------------------------------

    /**
     * Recursively merges {@code other} into this tree. For overlapping keys, child subtrees are merged in turn.
     * For keys present only in {@code other}, the corresponding subtree reference is adopted directly.
     */
    public void addTree(final Tree<T> other) {
        other.children.forEach((k, t) -> {
            if (this.children.containsKey(k)) {
                this.children.get(k).addTree(t);
            } else {
                this.children.put(k, t);
            }
        });
    }

    /**
     * Splits this tree into one tree per root key. If the tree has a single root, returns a singleton list
     * containing this tree.
     */
    public List<Tree<T>> splitParents() {
        if (this.children.size() == 1) {
            return Collections.singletonList(this);
        }
        final List<Tree<T>> parents = new ArrayList<>();
        this.children.forEach((k, t) -> {
            final Tree<T> parentTree = new Tree<>();
            parentTree.children.put(k, t);
            parents.add(parentTree);
        });
        return parents;
    }

    // ------------------------------------------------------------------
    // output
    // ------------------------------------------------------------------

    /**
     * Produces a formatted string representation of the tree structure using a {@code |--} ASCII style.
     */
    public String prettyPrint() {
        final StringBuilder builder = new StringBuilder();
        prettyPrint(builder, "");
        final String pretty = builder.toString();
        return pretty.isEmpty() ? pretty : pretty.substring(0, pretty.length() - System.lineSeparator().length());
    }

    private void prettyPrint(final StringBuilder builder, final String prefix) {
        for (final Map.Entry<T, Tree<T>> entry : this.children.entrySet()) {
            builder.append(prefix).append("|--").append(entry.getKey());
            builder.append(System.lineSeparator());
            entry.getValue().prettyPrint(builder, prefix + "   ");
        }
    }

    // ------------------------------------------------------------------
    // identity
    // ------------------------------------------------------------------

    /**
     * Structural recursive equality. Order across siblings is irrelevant; two trees are equal if they have the
     * same set of keys and each key's subtree is recursively equal.
     */
    @Override
    public boolean equals(final Object other) {
        if (this == other) return true;
        if (!(other instanceof Tree)) return false;
        final Tree<?> that = (Tree<?>) other;
        return this.children.equals(that.children);
    }

    @Override
    public int hashCode() {
        return this.children.hashCode();
    }

    @Override
    public String toString() {
        return this.children.toString();
    }
}
