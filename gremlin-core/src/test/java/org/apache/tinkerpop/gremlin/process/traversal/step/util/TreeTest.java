/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TreeTest extends StepTest {

    @Test
    public void shouldProvideValidDepths() {
        Tree<String> tree = new Tree<String>();
        tree.put("marko", new Tree<String>(TreeTest.createTree("a", new Tree<String>("a1", "a2")), TreeTest.createTree("b", new Tree<String>("b1", "b2", "b3"))));
        tree.put("josh", new Tree<String>("1", "2"));

        assertEquals(0, tree.getObjectsAtDepth(0).size());
        assertEquals(2, tree.getObjectsAtDepth(1).size());
        assertEquals(4, tree.getObjectsAtDepth(2).size());
        assertEquals(5, tree.getObjectsAtDepth(3).size());
        assertEquals(0, tree.getObjectsAtDepth(4).size());
        assertEquals(0, tree.getObjectsAtDepth(5).size());

        assertEquals(2, tree.get("josh").size());
        assertEquals(0, tree.get("marko").get("b").get("b1").size());
        assertEquals(3, tree.get("marko").get("b").size());
        assertNull(tree.get("marko").get("c"));
    }

    @Test
    public void shouldProvideValidLeaves() {
        Tree<String> tree = new Tree<String>();
        tree.put("marko", new Tree<String>(TreeTest.createTree("a", new Tree<String>("a1", "a2")), TreeTest.createTree("b", new Tree<String>("b1", "b2", "b3"))));
        tree.put("josh", new Tree<String>("1", "2"));

        assertEquals(7, tree.getLeafTrees().size());
        for (Tree<String> t : tree.getLeafTrees()) {
            assertEquals(1, t.keySet().size());
            final String key = t.keySet().iterator().next();
            assertTrue(Arrays.asList("a1", "a2", "b1", "b2", "b3", "1", "2").contains(key));
        }

        assertEquals(7, tree.getLeafObjects().size());
        for (String s : tree.getLeafObjects()) {
            assertTrue(Arrays.asList("a1", "a2", "b1", "b2", "b3", "1", "2").contains(s));
        }
    }

    @Test
    public void shouldMergeTreesCorrectly() {
        Tree<String> tree1 = new Tree<>();
        tree1.put("1", new Tree<String>(TreeTest.createTree("1_1", new Tree<String>("1_1_1")), TreeTest.createTree("1_2", new Tree<String>("1_2_1"))));
        Tree<String> tree2 = new Tree<>();
        tree2.put("1", new Tree<String>(TreeTest.createTree("1_1", new Tree<String>("1_1_1")), TreeTest.createTree("1_2", new Tree<String>("1_2_2"))));

        Tree<String> mergeTree = new Tree<>();
        mergeTree.addTree(tree1);
        mergeTree.addTree(tree2);

        assertEquals(1, mergeTree.size());
        assertEquals(0, mergeTree.getObjectsAtDepth(0).size());
        assertEquals(1, mergeTree.getObjectsAtDepth(1).size());
        assertEquals(2, mergeTree.getObjectsAtDepth(2).size());
        assertEquals(3, mergeTree.getObjectsAtDepth(3).size());
        assertTrue(mergeTree.getObjectsAtDepth(3).contains("1_1_1"));
        assertTrue(mergeTree.getObjectsAtDepth(3).contains("1_2_1"));
        assertTrue(mergeTree.getObjectsAtDepth(3).contains("1_2_2"));
    }

    @Test
    public void testPrettyPrintSingleNode() {
        final Tree<String> tree = new Tree<>();
        tree.put("root", new Tree<>());

        final String expected = "|--root";
        assertEquals(expected, tree.prettyPrint());
    }

    @Test
    public void testPrettyPrintMultipleNodes() {
        final Tree<String> tree = new Tree<>();
        final Tree<String> child1 = new Tree<>();
        final Tree<String> child2 = new Tree<>();
        tree.put("root", new Tree<>());
        tree.get("root").put("child1", child1);
        tree.get("root").put("child2", child2);

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
        final Tree<String> child1 = new Tree<>();
        final Tree<String> grandchild = new Tree<>();
        tree.put("root", new Tree<>());
        tree.get("root").put("child1", child1);
        tree.get("root").get("child1").put("grandchild", grandchild);

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

    private static <T> Map.Entry<T, Tree<T>> createTree(T key, Tree<T> tree) {
        return new AbstractMap.SimpleEntry<>(key, tree);
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
