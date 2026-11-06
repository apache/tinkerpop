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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TreeTest extends StepTest {

    /**
     * Helper that builds a subtree shaped like {@code key -> subtree}.
     */
    private static <T> Tree<T> branch(final T key, final Tree<T> subtree) {
        final Tree<T> wrapper = new Tree<>();
        wrapper.getOrCreateChild(key).addTree(subtree);
        return wrapper;
    }

    @Test
    public void shouldProvideValidDepths() {
        final Tree<String> tree = new Tree<>();
        final Tree<String> markoSubtree = new Tree<>();
        markoSubtree.addTree(branch("a", new Tree<>("a1", "a2")));
        markoSubtree.addTree(branch("b", new Tree<>("b1", "b2", "b3")));
        tree.getOrCreateChild("marko").addTree(markoSubtree);
        tree.getOrCreateChild("josh").addTree(new Tree<>("1", "2"));

        assertEquals(2, tree.getNodesAtDepth(0).size());
        assertTrue(tree.getNodesAtDepth(0).containsAll(Arrays.asList("marko", "josh")));
        assertEquals(4, tree.getNodesAtDepth(1).size());
        assertEquals(5, tree.getNodesAtDepth(2).size());
        assertEquals(0, tree.getNodesAtDepth(3).size());
        assertEquals(0, tree.getNodesAtDepth(4).size());
        assertEquals(0, tree.getNodesAtDepth(5).size());

        assertEquals(1, tree.getTreesAtDepth(0).size());
        assertEquals(tree, tree.getTreesAtDepth(0).get(0));

        assertEquals(2, tree.childAt("josh").rootNodes().size());
        assertTrue(tree.childAt("marko").childAt("b").childAt("b1").isLeaf());
        assertEquals(3, tree.childAt("marko").childAt("b").rootNodes().size());
        assertFalse(tree.childAt("marko").hasChild("c"));
    }

    @Test
    public void shouldProvideValidLeaves() {
        final Tree<String> tree = new Tree<>();
        final Tree<String> markoSubtree = new Tree<>();
        markoSubtree.addTree(branch("a", new Tree<>("a1", "a2")));
        markoSubtree.addTree(branch("b", new Tree<>("b1", "b2", "b3")));
        tree.getOrCreateChild("marko").addTree(markoSubtree);
        tree.getOrCreateChild("josh").addTree(new Tree<>("1", "2"));

        assertEquals(7, tree.getLeafTrees().size());
        for (final Tree<String> t : tree.getLeafTrees()) {
            assertEquals(1, t.rootNodes().size());
            final String key = t.rootNodes().iterator().next();
            assertTrue(Arrays.asList("a1", "a2", "b1", "b2", "b3", "1", "2").contains(key));
        }

        assertEquals(7, tree.getLeafNodes().size());
        for (final String s : tree.getLeafNodes()) {
            assertTrue(Arrays.asList("a1", "a2", "b1", "b2", "b3", "1", "2").contains(s));
        }
    }

    @Test
    public void shouldMergeTreesCorrectly() {
        final Tree<String> tree1 = new Tree<>();
        final Tree<String> tree1OneSubtree = new Tree<>();
        tree1OneSubtree.addTree(branch("1_1", new Tree<>("1_1_1")));
        tree1OneSubtree.addTree(branch("1_2", new Tree<>("1_2_1")));
        tree1.getOrCreateChild("1").addTree(tree1OneSubtree);

        final Tree<String> tree2 = new Tree<>();
        final Tree<String> tree2OneSubtree = new Tree<>();
        tree2OneSubtree.addTree(branch("1_1", new Tree<>("1_1_1")));
        tree2OneSubtree.addTree(branch("1_2", new Tree<>("1_2_2")));
        tree2.getOrCreateChild("1").addTree(tree2OneSubtree);

        final Tree<String> mergeTree = new Tree<>();
        mergeTree.addTree(tree1);
        mergeTree.addTree(tree2);

        assertEquals(1, mergeTree.rootNodes().size());
        assertEquals(1, mergeTree.getNodesAtDepth(0).size());
        assertEquals(2, mergeTree.getNodesAtDepth(1).size());
        assertEquals(3, mergeTree.getNodesAtDepth(2).size());
        assertEquals(0, mergeTree.getNodesAtDepth(3).size());
        assertTrue(mergeTree.getNodesAtDepth(2).contains("1_1_1"));
        assertTrue(mergeTree.getNodesAtDepth(2).contains("1_2_1"));
        assertTrue(mergeTree.getNodesAtDepth(2).contains("1_2_2"));
    }

    @Test
    public void shouldGetOrCreateChild() {
        final Tree<String> tree = new Tree<>();
        final Tree<String> child = tree.getOrCreateChild("a");
        assertNotNull(child);
        assertTrue(child.isLeaf());
        // calling again returns the same instance
        assertSame(child, tree.getOrCreateChild("a"));

        // mutating the returned subtree is observable through the parent
        child.getOrCreateChild("a1");
        assertTrue(tree.childAt("a").hasChild("a1"));
    }

    @Test
    public void shouldAllowNullKeys() {
        // HashMap allows null keys; the new Tree must preserve this (TreeSideEffectStep relies on it).
        final Tree<Object> tree = new Tree<>();
        final Tree<Object> nullChild = tree.getOrCreateChild(null);
        assertNotNull(nullChild);
        assertTrue(tree.hasChild(null));
        assertEquals(1, tree.rootNodes().size());
    }

    @Test
    public void shouldThrowOnMissingChildAt() {
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("a");
        try {
            tree.childAt("missing");
            fail("expected IllegalArgumentException for missing key");
        } catch (final IllegalArgumentException iae) {
            assertThat(iae.getMessage(), Matchers.containsString("missing"));
        }
    }

    @Test
    public void shouldDetectImmediateAndRecursiveContainment() {
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("root").getOrCreateChild("child").getOrCreateChild("grandchild");

        assertTrue(tree.hasChild("root"));
        assertFalse(tree.hasChild("child"));
        assertFalse(tree.hasChild("grandchild"));

        assertTrue(tree.contains("root"));
        assertTrue(tree.contains("child"));
        assertTrue(tree.contains("grandchild"));
        assertFalse(tree.contains("nope"));
    }

    @Test
    public void shouldFindSubtreeRecursively() {
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("root").getOrCreateChild("child").getOrCreateChild("grandchild");

        final Optional<Tree<String>> found = tree.findSubtree("grandchild");
        assertTrue(found.isPresent());
        assertTrue(found.get().isLeaf());

        assertFalse(tree.findSubtree("missing").isPresent());
    }

    @Test
    public void shouldReportNodeCount() {
        final Tree<String> tree = new Tree<>();
        assertEquals(0, tree.nodeCount());
        tree.getOrCreateChild("a");
        assertEquals(1, tree.nodeCount());
        tree.getOrCreateChild("b").getOrCreateChild("b1");
        assertEquals(3, tree.nodeCount());
    }

    @Test
    public void shouldReportLeafForEmptyTree() {
        assertTrue(new Tree<String>().isLeaf());

        // a node with children is not a leaf
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("a");
        assertFalse(tree.isLeaf());
    }

    @Test
    public void shouldBeStructurallyEqualOrderInsensitive() {
        final Tree<String> a = new Tree<>();
        a.getOrCreateChild("x").getOrCreateChild("x1");
        a.getOrCreateChild("y");

        final Tree<String> b = new Tree<>();
        b.getOrCreateChild("y");
        b.getOrCreateChild("x").getOrCreateChild("x1");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        final Tree<String> c = new Tree<>();
        c.getOrCreateChild("x");
        c.getOrCreateChild("y");
        assertFalse(a.equals(c));
    }

    @Test
    public void shouldSplitParents() {
        final Tree<String> single = new Tree<>();
        single.getOrCreateChild("only").getOrCreateChild("child");
        final List<Tree<String>> singleSplit = single.splitParents();
        assertEquals(1, singleSplit.size());
        assertEquals(single, singleSplit.get(0));

        final Tree<String> multi = new Tree<>();
        multi.getOrCreateChild("a").getOrCreateChild("a1");
        multi.getOrCreateChild("b").getOrCreateChild("b1");
        final List<Tree<String>> multiSplit = multi.splitParents();
        assertEquals(2, multiSplit.size());
        for (final Tree<String> parent : multiSplit) {
            assertEquals(1, parent.rootNodes().size());
        }
    }

    @Test
    public void testPrettyPrintSingleNode() {
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("root");

        final String expected = "|--root";
        assertEquals(expected, tree.prettyPrint());
    }

    @Test
    public void testPrettyPrintMultipleNodes() {
        final Tree<String> tree = new Tree<>();
        final Tree<String> root = tree.getOrCreateChild("root");
        root.getOrCreateChild("child1");
        root.getOrCreateChild("child2");

        // either can be expected since Tree doesn't maintain node order
        final String expected1 = "|--root" + System.lineSeparator() +
                                 "   |--child1" + System.lineSeparator() +
                                 "   |--child2";
        final String expected2 = "|--root" + System.lineSeparator() +
                                 "   |--child2" + System.lineSeparator() +
                                 "   |--child1";
        assertThat(tree.prettyPrint(), Matchers.anyOf(Matchers.is(expected1), Matchers.is(expected2)));
    }

    @Test
    public void testPrettyPrintNestedTree() {
        final Tree<String> tree = new Tree<>();
        tree.getOrCreateChild("root").getOrCreateChild("child1").getOrCreateChild("grandchild");

        final String expected = "|--root" + System.lineSeparator() +
                                "   |--child1" + System.lineSeparator() +
                                "      |--grandchild";
        assertEquals(expected, tree.prettyPrint());
    }

    @Test
    public void testPrettyPrintEmptyTree() {
        final Tree<String> tree = new Tree<>();
        final String expected = "";
        assertEquals(expected, tree.prettyPrint());
    }

    @Test
    public void shouldRenderToStringAsUnderlyingMap() {
        assertEquals("{}", new Tree<String>().toString());

        final Tree<String> single = new Tree<>();
        single.getOrCreateChild("root");
        assertEquals("{root={}}", single.toString());

        final Tree<String> nested = new Tree<>();
        nested.getOrCreateChild("root").getOrCreateChild("child");
        assertEquals("{root={child={}}}", nested.toString());
    }

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.tree(),
                __.tree().by("name").by("age"),
                __.tree().by("age").by("name")
        );
    }
}
