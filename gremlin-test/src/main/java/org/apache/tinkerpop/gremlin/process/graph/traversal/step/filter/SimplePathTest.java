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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.Path;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SimplePathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inXcreatedX_simplePath() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("josh") || vertex.value("name").equals("peter"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXboth_simplePathX_timesX3X_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_repeatXboth_simplePathX_timesX3X_path();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertTrue(traversal.next().isSimple());
        }
        assertEquals(18, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends SimplePathTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").simplePath();
        }


        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path() {
            return g.V().repeat(both().simplePath()).times(3).path();
        }
    }

    public static class ComputerTest extends SimplePathTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_simplePath(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").simplePath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXboth_simplePathX_timesX3X_path() {
            return g.V().repeat(both().simplePath()).times(3).path();
        }
    }
}
