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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import java.util.HashSet;
import java.util.Set;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class FilterTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXisX0XX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXconstantX0XX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXisX0XX();

    public abstract Traversal<Edge, Edge> get_g_E_filterXconstantX0XX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXisX0XX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXisX0XX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_filterXconstantX0XX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_filterXconstantX0XX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXisX0XX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXisX0XX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_filterXconstantX0XX() {
        final Traversal<Edge, Edge> traversal = get_g_E_filterXconstantX0XX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, edges.size());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends FilterTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXisX0XX() {
            return g.V().filter(is(0));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_filterXconstantX0XX() {
            return g.V().filter(constant(0));
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXisX0XX() {
            return g.E().filter(is(0));
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_filterXconstantX0XX() {
            return g.E().filter(constant(0));
        }
    }
}
