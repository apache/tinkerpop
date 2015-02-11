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
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SampleTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Edge, Edge> get_g_E_sampleX1X();

    public abstract Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX();

    public abstract Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_sampleX1X() {
        final Traversal<Edge, Edge> traversal = get_g_E_sampleX1X();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_sampleX2X_byXweightX() {
        final Traversal<Edge, Edge> traversal = get_g_E_sampleX2X_byXweightX();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_sampleX1X_byXweightXX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_localXoutE_sampleX1X_byXweightXX();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends SampleTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            return g.E().sample(1);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            return g.E().sample(2).by("weight");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            return g.V().local(outE().sample(1).by("weight"));
        }
    }

    public static class ComputerTest extends SampleTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            return g.E().sample(1);   // TODO: makes no sense when its global
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            return g.E().sample(2).by("weight"); // TODO: makes no sense when its global
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            return g.V().local(outE().sample(1).by("weight")).submit(g.compute());
        }
    }
}