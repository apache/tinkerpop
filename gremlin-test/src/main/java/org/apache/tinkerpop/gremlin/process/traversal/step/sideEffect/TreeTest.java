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
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class TreeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_tree();

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX();

    public abstract Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Tree> get_g_V_out_tree_byXageX();

    public abstract Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id);

    public abstract Traversal<Vertex, Tree> get_g_V_out_out_out_tree();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_out_tree_byXnameX() {
        final Traversal<Vertex, Tree> traversal = get_g_VX1X_out_out_tree_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_tree_byXageX() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_tree_byXageX();
        printTraversalForm(traversal);

        final Tree tree = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(3, tree.size());
        assertTrue(tree.containsKey(29));
        assertEquals(2, ((Map) tree.get(29)).size());
        assertTrue(((Map) tree.get(29)).containsKey(32));
        assertTrue(((Map) tree.get(29)).containsKey(27));
        assertTrue(tree.containsKey(35));
        assertEquals(0, ((Map) tree.get(35)).size());
        assertTrue(tree.containsKey(32));
        assertEquals(0, ((Map) tree.get(32)).size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX() {
        final Traversal<Vertex, Tree> traversal = get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertCommonA(traversal);
    }

    private static void assertCommonA(final Traversal<Vertex, Tree> traversal) {
        final Tree tree = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, tree.size());
        assertTrue(tree.containsKey("marko"));
        assertEquals(1, ((Map) tree.get("marko")).size());
        assertTrue(((Map) tree.get("marko")).containsKey("josh"));
        assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("lop"));
        assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("ripple"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_tree_byXidX() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_out_tree_byXidX();
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_treeXaX_byXidX_capXaX() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_out_treeXaX_byXidX_capXaX();
        printTraversalForm(traversal);
        assertCommonB(traversal);
    }

    private void assertCommonB(Traversal<Vertex, Tree> traversal) {
        final Tree tree = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, tree.size());
        assertTrue(tree.containsKey(convertToVertexId("marko")));
        assertEquals(1, ((Map) tree.get(convertToVertexId("marko"))).size());
        assertTrue(((Map) tree.get(convertToVertexId("marko"))).containsKey(convertToVertexId("josh")));
        assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("lop")));
        assertTrue(((Map) ((Map) tree.get(convertToVertexId("marko"))).get(convertToVertexId("josh"))).containsKey(convertToVertexId("ripple")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_tree() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_out_tree();
        printTraversalForm(traversal);
        assertCommonC(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_treeXaX_capXaX() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_out_treeXaX_capXaX();
        printTraversalForm(traversal);
        assertCommonC(traversal);
    }

    private void assertCommonC(Traversal<Vertex, Tree> traversal) {
        final Tree tree = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, tree.size());
        assertTrue(tree.containsKey(convertToVertex(graph, "marko")));
        assertEquals(1, ((Map) tree.get(convertToVertex(graph, "marko"))).size());
        assertTrue(((Map) tree.get(convertToVertex(graph, "marko"))).containsKey(convertToVertex(graph, "josh")));
        assertTrue(((Map) ((Map) tree.get(convertToVertex(graph, "marko"))).get(convertToVertex(graph, "josh"))).containsKey(convertToVertex(graph, "lop")));
        assertTrue(((Map) ((Map) tree.get(convertToVertex(graph, "marko"))).get(convertToVertex(graph, "josh"))).containsKey(convertToVertex(graph, "ripple")));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_out_tree() {
        final Traversal<Vertex, Tree> traversal = get_g_V_out_out_out_tree();
        printTraversalForm(traversal);
        final Tree tree = traversal.next();
        assertTrue(tree.isEmpty());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends TreeTest {
        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_tree_byXnameX(final Object v1Id) {
            return g.V(v1Id).out().out().tree().by("name");
        }

        public Traversal<Vertex, Tree> get_g_V_out_tree_byXageX() {
            return g.V().out().tree().by("age");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX(final Object v1Id) {
            return g.V(v1Id).out().out().tree("a").by("name").both().both().cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree_byXidX() {
            return g.V().out().out().tree().by(T.id);
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_byXidX_capXaX() {
            return g.V().out().out().tree("a").by(T.id).cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_tree() {
            return g.V().out().out().tree();
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_treeXaX_capXaX() {
            return g.V().out().out().tree("a").cap("a");
        }

        @Override
        public Traversal<Vertex, Tree> get_g_V_out_out_out_tree() {
            return g.V().out().out().out().tree();
        }
    }
}
