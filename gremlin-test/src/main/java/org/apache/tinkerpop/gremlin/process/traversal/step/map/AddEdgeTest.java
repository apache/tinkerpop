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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
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

    public abstract Traversal<Vertex, Edge> get_g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX();

    public abstract Traversal<Vertex, Edge> get_g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X(final Vertex a, final Vertex v2);

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
        assertEquals(7, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
        assertEquals(7, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals(2.0d, g.E(edge).<Double>values("weight").next(), 0.00001d);
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            count++;


        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals(2.0d, g.E(edge).<Double>values("weight").next(), 0.00001d);
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            count++;


        }
        assertEquals(1, count);
        assertEquals(7, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals("now", g.E(edge).values("time").next());
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            count++;
        }
        assertEquals(36, count);
        assertEquals(42, IteratorUtils.count(g.E()));
        for (final Vertex vertex : IteratorUtils.list(g.V())) {
            assertEquals(6, g.V(vertex).out("existsWith").count().next().intValue());
            assertEquals(6, g.V(vertex).in("existsWith").count().next().intValue());
        }
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals("now", g.E(edge).values("time").next());
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            count++;
        }
        assertEquals(36, count);
        assertEquals(42, IteratorUtils.count(g.E()));
        for (final Vertex vertex : IteratorUtils.list(g.V())) {
            assertEquals(6, g.V(vertex).out("existsWith").count().next().intValue());
            assertEquals(6, g.V(vertex).in("existsWith").count().next().intValue());
        }
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals("codeveloper", edge.label());
            assertEquals(2009, g.E(edge).values("year").next());
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            assertEquals("person", g.E(edge).inV().label().next());
            assertEquals("person", g.E(edge).outV().label().next());
            assertFalse(g.E(edge).inV().values("name").next().equals("vadas"));
            assertFalse(g.E(edge).outV().values("name").next().equals("vadas"));
            assertFalse(g.E(edge).inV().next().equals(g.E(edge).outV().next()));
            count++;

        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals("codeveloper", edge.label());
            assertEquals(2009, g.E(edge).values("year").next());
            assertEquals(1, g.E(edge).properties().count().next().intValue());
            assertEquals("person", g.E(edge).inV().label().next());
            assertEquals("person", g.E(edge).outV().label().next());
            assertFalse(g.E(edge).inV().values("name").next().equals("vadas"));
            assertFalse(g.E(edge).outV().values("name").next().equals("vadas"));
            assertFalse(g.E(edge).inV().next().equals(g.E(edge).outV().next()));
            count++;

        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X() {
        final Vertex a = g.V().has("name", "marko").next();
        final Vertex b = g.V().has("name", "peter").next();

        final Traversal<Vertex, Edge> traversal = get_g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X(a, b);
        final Edge edge = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(edge.outVertex(), convertToVertex(graph, "marko"));
        assertEquals(edge.inVertex(), convertToVertex(graph, "peter"));
        assertEquals("knows", edge.label());
        assertEquals(1, g.E(edge).properties().count().next().intValue());
        assertEquals(0.5d, g.E(edge).<Double>values("weight").next(), 0.1d);
        assertEquals(6L, g.V().count().next().longValue());
        assertEquals(7L, g.E().count().next().longValue());
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
            assertEquals(2009, g.E(edge).values("year").next());
            assertEquals("public", g.E(edge).values("acl").next());
            assertEquals(2, g.E(edge).properties().count().next().intValue());
            assertEquals("person", g.E(edge).inV().label().next());
            assertEquals("software", g.E(edge).outV().label().next());
            if (g.E(edge).outV().values("name").next().equals("ripple"))
                assertEquals("josh", g.E(edge).inV().values("name").next());
            count++;

        }
        assertEquals(4, count);
        assertEquals(10, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
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
            assertEquals(2009, g.E(edge).values("year").next());
            assertEquals("public", g.E(edge).values("acl").next());
            assertEquals(2, g.E(edge).properties().count().next().intValue());
            assertEquals("person", g.E(edge).inV().label().next());
            assertEquals("software", g.E(edge).outV().label().next());
            if (g.E(edge).outV().values("name").next().equals("ripple"))
                assertEquals("josh", g.E(edge).inV().values("name").next());
            count++;

        }
        assertEquals(4, count);
        assertEquals(10, IteratorUtils.count(g.E()));
        assertEquals(6, IteratorUtils.count(g.V()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX() {
        final Traversal<Vertex, Edge> traversal = get_g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX();
        printTraversalForm(traversal);
        assertEquals("next", traversal.next().label());
        assertFalse(traversal.hasNext());
        assertEquals(6L, g.V().count().next().longValue());
        assertEquals(6L, g.E().count().next().longValue());
        assertEquals(Arrays.asList(2L, 2L, 2L, 2L, 2L, 2L), g.V().map(bothE().count()).toList());
        assertEquals(Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L), g.V().map(inE().count()).toList());
        assertEquals(Arrays.asList(1L, 1L, 1L, 1L, 1L, 1L), g.V().map(outE().count()).toList());
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
            return g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").addE("codeveloper").from("a").to("b").property("year", 2009);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX() {
            return g.V().as("a").in("created").addE("createdBy").from("a").property("year", 2009).property("acl", "public");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X(final Vertex a, final Vertex b) {
            return g.withSideEffect("b", b).V(a).addE("knows").to("b").property("weight", 0.5d);
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
            return g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").select("a", "b").addInE("a", "codeveloper", "b", "year", 2009);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX() {
            return g.V().as("a").in("created").addInE("createdBy", "a", "year", 2009, "acl", "public");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX() {
            return g.addV().as("first").repeat(__.addE("next").to(__.addV()).inV()).times(5).addE("next").to(select("first"));
        }
    }
}
