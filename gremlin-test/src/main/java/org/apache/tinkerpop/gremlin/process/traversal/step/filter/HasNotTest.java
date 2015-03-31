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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class HasNotTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey);

    public abstract Traversal<Vertex, String> get_g_V_hasNotXoutXcreatedXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasNotXprop() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasNotXprop(convertToVertexId("marko"), "circumference");
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasNotXprop(convertToVertexId("marko"), "name");
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasNotXprop() {
        Traversal<Vertex, Vertex> traversal = get_g_V_hasNotXprop("circumference");
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.toList();
        assertEquals(6, list.size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasNotXoutXcreatedXX() {
        Traversal<Vertex, String> traversal = get_g_V_hasNotXoutXcreatedXX();
        checkResults(Arrays.asList("vadas", "lop", "ripple"), traversal);
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends HasNotTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            return g.V(v1Id).hasNot(propertyKey);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            return g.V().hasNot(propertyKey);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasNotXoutXcreatedXX() {
            return g.V().hasNot(__.out("created")).values("name");
        }
    }
}