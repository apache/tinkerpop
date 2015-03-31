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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class HasTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_name();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXblahX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X();

    public abstract Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id);

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXknowsX();

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX();

    public abstract Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_V_outXknowsX_hasXoutXcreatedXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXkeyX() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "name");
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        //
        traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "circumference");
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXname_markoX() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_markoX();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_blahX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXage_gt_30X();
        printTraversalForm(traversal);
        final List<Vertex> list = traversal.toList();
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>value("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXage_gt_30X() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasXage_gt_30X(convertToVertexId("josh"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertexId("vadas"), traversal.next().id());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXblahX() {
        assumeTrue(graphMeetsTestRequirements());
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXblahX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_EX7X_hasXlabelXknowsX() {
        //System.out.println(convertToEdgeId("marko", "knows", "vadas"));
        Traversal<Edge, Edge> traversal = get_g_EX7X_hasLabelXknowsX(convertToEdgeId("marko", "knows", "vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_hasXlabelXknowsX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXknowsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_E_hasXlabelXuses_traversesX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXuses_traversesX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("uses") || label.equals("traverses"));
        }
        assertEquals(9, counter);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXlabelXperson_software_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXperson_software_blahX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("software") || label.equals("person"));
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXperson_name_markoX_age() {
        final Traversal<Vertex, Integer> traversal = get_g_V_hasXperson_name_markoX_age();
        printTraversalForm(traversal);
        assertEquals(29, traversal.next().intValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_hasXweight_inside_0_06X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        assertFalse(traversal.hasNext());
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends HasTest {
        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_name() {
            return g.V().out("knows").has(out("created")).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            return g.V(v1Id).has(key);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            return g.V(v1Id).has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).has("age", Compare.gt, 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().hasId(v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", Compare.gt, 30);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            return g.E(e7Id).hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            return g.E().hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            return g.E().hasLabel("uses", "traverses");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            return g.V().hasLabel("person", "software", "blah");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            return g.V().has("person", "name", "marko").values("age");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            return g.V(v1Id).outE().has("weight", Compare.inside, Arrays.asList(0.0d, 0.6d)).inV();
        }
    }
}