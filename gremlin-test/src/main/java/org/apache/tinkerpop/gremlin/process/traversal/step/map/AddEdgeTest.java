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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AddEdgeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X(final Object v1Id);

    public abstract Traversal<Vertex, Edge> get_g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX();

    public abstract Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X();

    public abstract Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX();

    ///////

    @Deprecated
    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id);

    @Deprecated
    public abstract Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id);

    @Deprecated
    public abstract Traversal<Vertex, Edge> get_g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX();

    @Deprecated
    public abstract Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X();

    @Deprecated
    public abstract Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX();

    @Test
    @LoadGraphWith(MODERN)
    @Deprecated
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(0, IteratorUtils.count(edge.properties()));
            count++;

        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(0, IteratorUtils.count(edge.properties()));
            count++;

        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @Deprecated
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(2.0d, edge.<Double>value("weight").doubleValue(),0.00001d);
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;


        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X() {
        final Traversal<Vertex, Edge> traversal = get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(2.0d, edge.<Double>value("weight").doubleValue(),0.00001d);
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;


        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @Ignore
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX() {
        final Traversal<Vertex, Edge> traversal = get_g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("existsWith", edge.label());
            assertEquals("now", edge.value("time"));
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;
        }
        assertEquals(36, count);
        assertEquals(42, IteratorUtils.count(graph.edges()));
        for (final Vertex vertex : IteratorUtils.list(graph.vertices())) {
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.OUT, "existsWith")));
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.IN, "existsWith")));
        }
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("existsWith", edge.label());
            assertEquals("now", edge.value("time"));
            assertEquals(1, IteratorUtils.count(edge.properties()));
            count++;
        }
        assertEquals(36, count);
        assertEquals(42, IteratorUtils.count(graph.edges()));
        for (final Vertex vertex : IteratorUtils.list(graph.vertices())) {
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.OUT, "existsWith")));
            assertEquals(6, IteratorUtils.count(vertex.edges(Direction.IN, "existsWith")));
        }
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @Deprecated
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X() {
        final Traversal<Vertex, Edge> traversal = get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("co-developer", edge.label());
            assertEquals(2009, (int) edge.value("year"));
            assertEquals(1, IteratorUtils.count(edge.properties()));
            assertEquals("person", edge.inVertex().label());
            assertEquals("person", edge.outVertex().label());
            assertFalse(edge.inVertex().value("name").equals("vadas"));
            assertFalse(edge.outVertex().value("name").equals("vadas"));
            assertFalse(edge.inVertex().equals(edge.outVertex()));
            count++;

        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X() {
        final Traversal<Vertex, Edge> traversal = get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("co-developer", edge.label());
            assertEquals(2009, (int) edge.value("year"));
            assertEquals(1, IteratorUtils.count(edge.properties()));
            assertEquals("person", edge.inVertex().label());
            assertEquals("person", edge.outVertex().label());
            assertFalse(edge.inVertex().value("name").equals("vadas"));
            assertFalse(edge.outVertex().value("name").equals("vadas"));
            assertFalse(edge.inVertex().equals(edge.outVertex()));
            count++;

        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @Deprecated
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(2009, (int) edge.value("year"));
            assertEquals("public", edge.value("acl"));
            assertEquals(2, IteratorUtils.count(edge.properties()));
            assertEquals("person", edge.inVertex().label());
            assertEquals("software", edge.outVertex().label());
            if (edge.outVertex().value("name").equals("ripple"))
                assertEquals("josh", edge.inVertex().value("name"));
            count++;

        }
        assertEquals(4, count);
        assertEquals(10, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("createdBy", edge.label());
            assertEquals(2009, (int) edge.value("year"));
            assertEquals("public", edge.value("acl"));
            assertEquals(2, IteratorUtils.count(edge.properties()));
            assertEquals("person", edge.inVertex().label());
            assertEquals("software", edge.outVertex().label());
            if (edge.outVertex().value("name").equals("ripple"))
                assertEquals("josh", edge.inVertex().value("name"));
            count++;

        }
        assertEquals(4, count);
        assertEquals(10, IteratorUtils.count(graph.edges()));
        assertEquals(6, IteratorUtils.count(graph.vertices()));
    }

    public static class Traversals extends AddEdgeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addE("createdBy").to("a");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addE("createdBy").to("a").property("weight", 2.0d);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX() {
            return g.V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X() {
            return g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").addE("co-developer").from("a").to("b").property("year", 2009);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX() {
            return g.V().as("a").in("created").addE("createdBy").from("a").property("year", 2009).property("acl", "public");
        }

        ///////

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X(final Object v1Id) {
            return g.V(v1Id).as("a").out("created").addOutE("createdBy", "a", "weight", 2.0d);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX() {
            return g.withSideEffect("x", g.V().toList()).V().addOutE("existsWith", "x", "time", "now");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X() {
            return g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").select("a", "b").addInE("a", "co-developer", "b", "year", 2009);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX() {
            return g.V().as("a").in("created").addInE("createdBy", "a", "year", 2009, "acl", "public");
        }
    }
}
