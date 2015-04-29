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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.*;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TreeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_tree();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX();

    public abstract Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id);

    @Test
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_out_tree_byXnameX() {
        final List<Traversal<Vertex, Tree>> traversals = Arrays.asList(
                get_g_VX1X_out_out_tree_byXnameX(convertToVertexId("marko")),
                get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(convertToVertexId("marko")));
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey("marko"));
            assertEquals(1, ((Map) tree.get("marko")).size());
            assertTrue(((Map) tree.get("marko")).containsKey("josh"));
            assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("lop"));
            assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("ripple"));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_tree_byXidX() {
        final List<Traversal<Vertex, Tree>> traversals = Arrays.asList(get_g_V_out_out_tree_byXidX(), get_g_V_out_out_treeXaX_byXidX_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey(convertToVertexId("marko")));
            assertEquals(1, ((Map) tree.get(convertToVertexId("marko"))).size());
            assertTrue(((Map) tree.get(convertToVertexId("marko"))).containsKey(convertToVertexId("josh")));
            assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("lop")));
            assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("ripple")));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_treeXaX_capXaX() {
        final List<Traversal<Vertex, Tree>> traversals = Arrays.asList(get_g_V_out_out_tree(), get_g_V_out_out_treeXaX_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(1, tree.size());
            assertTrue(tree.containsKey(convertToVertex(graph, "marko")));
            assertEquals(1, ((Map) tree.get(convertToVertex(graph, "marko"))).size());
            assertTrue(((Map) tree.get(convertToVertex(graph, "marko"))).containsKey(convertToVertex(graph, "josh")));
            assertTrue(((Map) ((Map) tree.get(convertToVertex(graph, "marko"))).get(convertToVertex(graph, "josh"))).containsKey(convertToVertex(graph, "lop")));
            assertTrue(((Map) ((Map) tree.get(convertToVertex(graph, "marko"))).get(convertToVertex(graph, "josh"))).containsKey(convertToVertex(graph, "ripple")));
        });
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends TreeTest {
        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            return (Traversal) g.V(v1Id).out().out().tree().by("name");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            return g.V(v1Id).out().out().tree("a").by("name").both().both().cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            return (Traversal) g.V().out().out().tree().by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX() {
            return (Traversal) g.V().out().out().tree("a").by(T.id).cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree() {
            return (Traversal) g.V().out().out().tree();
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX() {
            return (Traversal) g.V().out().out().tree("a").cap("a");
        }
    }
}
