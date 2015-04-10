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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class RangeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X();

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_out_limitX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_limitX2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_localXoutE_limitX1X_inVX_limitX3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_localXoutE_limitX1X_inVX_limitX3X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("lop") || name.equals("ripple"));
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void g_V_repeatXbothX_timesX3X_rangeX5_11X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXbothX_timesX3X_rangeX5_11X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            traversal.next();
            counter++;
        }
        assertEquals(6, counter);
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends RangeTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_limitX2X(final Object v1Id) {
            return g.V(v1Id).out().limit(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_localXoutE_limitX1X_inVX_limitX3X() {
            return g.V().local(outE().limit(1)).inV().limit(3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV(final Object v1Id) {
            return g.V(v1Id).out("knows").outE("created").range(0, 1).inV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X(final Object v1Id) {
            return g.V(v1Id).out("knows").out("created").range(0, 1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X(final Object v1Id) {
            return g.V(v1Id).out("created").in("created").range(1, 3);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV(final Object v1Id) {
            return g.V(v1Id).out("created").inE("created").range(1, 3).outV();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXbothX_timesX3X_rangeX5_11X() {
            return g.V().repeat(both()).times(3).range(5, 11);
        }
    }
}