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
package com.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.apache.tinkerpop.gremlin.LoadGraphWith;
import com.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.apache.tinkerpop.gremlin.process.Path;
import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CyclicPathTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(final Object v1);

    public abstract Traversal<Vertex, Path> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inXcreatedX_cyclicPath() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertEquals("marko", vertex.<String>value("name"));
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Path path = traversal.next();
            assertFalse(path.isSimple());
        }
        assertEquals(1, counter);
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends CyclicPathTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").cyclicPath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").cyclicPath().path();
        }
    }

    public static class ComputerTest extends CyclicPathTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").cyclicPath().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").cyclicPath().path().submit(g.compute());
        }
    }
}
