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
package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class HasNotTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey);

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
    public void get_g_V_hasNotXprop() {
        Traversal<Vertex, Vertex> traversal = get_g_V_hasNotXprop("circumference");
        printTraversalForm(traversal);
        final List<Element> list = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, list.size());
    }

    public static class StandardTest extends HasNotTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            return g.V(v1Id).hasNot(propertyKey);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            return g.V().hasNot(propertyKey);
        }
    }

    public static class ComputerTest extends HasNotTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            return g.V(v1Id).hasNot(propertyKey).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            return g.V().hasNot(propertyKey).submit(g.compute());
        }
    }
}