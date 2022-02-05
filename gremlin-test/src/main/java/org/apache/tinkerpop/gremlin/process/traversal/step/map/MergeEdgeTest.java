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

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(GremlinProcessRunner.class)
public abstract class MergeEdgeTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Edge> get_g_V_mergeEXlabel_self_weight_05X();

    public abstract Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX();

    public abstract Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists();

    public abstract Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX();

    public abstract Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists();

    public abstract Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated();

    public abstract Traversal<Vertex, Edge> get_g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated();

    public abstract Traversal<Map<Object,Object>, Edge> get_g_injectXlabel_knows_out_marko_in_vadasX_mergeE();

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_V_mergeEXlabel_self_weight_05X() {
        g.addV("person").property("name", "stephen").iterate();
        final Traversal<Vertex, Edge> traversal = get_g_V_mergeEXlabel_self_weight_05X();
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("self", edge.label());
        assertEquals(0.5d, edge.<Double>value("weight").doubleValue(), 0.0001d);
        assertFalse(traversal.hasNext());
        assertEquals(1, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_mergeEXlabel_knows_out_marko_in_vadasX() {
        g.addV("person").property(T.id, 100).property("name", "marko").
                addV("person").property(T.id, 101).property("name", "vadas").iterate();
        final Traversal<Edge, Edge> traversal = get_g_mergeEXlabel_knows_out_marko_in_vadasX();
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(100, edge.outVertex().id());
        assertEquals(101, edge.inVertex().id());
        assertFalse(traversal.hasNext());
        assertEquals(1, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists() {
        g.addV("person").property(T.id, 100).property("name", "marko").as("a").
                addV("person").property(T.id, 101).property("name", "vadas").as("b").
                addE("knows").from("a").to("b").iterate();
        final Traversal<Edge, Edge> traversal = get_g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists();
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(100, edge.outVertex().id());
        assertEquals(101, edge.inVertex().id());
        assertEquals(0.5d, edge.<Double>value("weight").doubleValue(), 0.0001d);
        assertFalse(traversal.hasNext());
        assertEquals(2, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX() {
        final Traversal<Edge, Edge> traversal = get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX();
        printTraversalForm(traversal);
        try {
            traversal.next();
            fail("Should have failed as vertices are not created");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), endsWith("could not be found and edge could not be created"));
        }
        assertEquals(0, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists() {
        g.addV("person").property(T.id, 100).property("name", "marko").as("a").
                addV("person").property(T.id, 101).property("name", "vadas").as("b").
                addE("knows").from("a").to("b").iterate();
        final Traversal<Edge, Edge> traversal = get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists();
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(100, edge.outVertex().id());
        assertEquals(101, edge.inVertex().id());
        assertEquals("N", edge.<String>value("created"));
        assertFalse(traversal.hasNext());
        assertEquals(1, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated() {
        g.addV("person").property(T.id, 100).property("name", "marko").as("a").
                addV("person").property(T.id, 101).property("name", "vadas").as("b").
                addE("knows").from("a").to("b").property("created","Y").iterate();
        final Traversal<Edge, Edge> traversal = get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated();
        printTraversalForm(traversal);
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(100, edge.outVertex().id());
        assertEquals(101, edge.inVertex().id());
        assertEquals("N", edge.<String>value("created"));
        assertFalse(traversal.hasNext());
        assertEquals(1, IteratorUtils.count(g.E()));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated() {
        g.addV("person").property(T.id, 100).property("name", "marko").as("a").
                addV("person").property(T.id, 101).property("name", "vadas").as("b").
                addE("knows").from("a").to("b").property("created","Y").
                addE("knows").from("a").to("b").iterate();
        final Traversal<Vertex, Edge> traversal = get_g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated();
        printTraversalForm(traversal);

        assertEquals(2, IteratorUtils.count(traversal));
        assertEquals(2, IteratorUtils.count(g.V()));
        assertEquals(2, IteratorUtils.count(g.E()));
        assertEquals(2, IteratorUtils.count(g.E().hasLabel("knows").has("created", "N")));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void g_injectXlabel_knows_out_marko_in_vadasX_mergeE() {
        g.addV("person").property(T.id, 100).property("name", "marko").
                addV("person").property(T.id, 101).property("name", "vadas").iterate();
        final Traversal<Map<Object,Object>, Edge> traversal = get_g_injectXlabel_knows_out_marko_in_vadasX_mergeE();
        printTraversalForm(traversal);

        assertEquals(1, IteratorUtils.count(traversal));
        assertEquals(2, IteratorUtils.count(g.V()));
        assertEquals(1, IteratorUtils.count(g.E()));
        assertEquals(1, IteratorUtils.count(g.V(100).out("knows").hasId(101)));
    }

    public static class Traversals extends MergeEdgeTest {

        @Override
        public Traversal<Vertex, Edge> get_g_V_mergeEXlabel_self_weight_05X() {
            return g.V().mergeE(asMap(T.label, "self", "weight", 0.5d));
        }

        @Override
        public Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX() {
            return g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100)));
        }

        @Override
        public Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists() {
            return g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "weight", 0.5d));
        }

        @Override
        public Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX() {
            return g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100))).
                    option(Merge.onCreate, asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "created", "Y")).
                    option(Merge.onMatch, asMap("created", "N"));
        }

        @Override
        public Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists() {
            return g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100))).
                    option(Merge.onCreate, asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "created", "Y")).
                    option(Merge.onMatch, asMap("created", "N"));
        }

        @Override
        public Traversal<Edge, Edge> get_g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated() {
            return g.mergeE(asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100))).
                    option(Merge.onCreate, asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "created", "Y")).
                    option(Merge.onMatch, asMap("created", "N"));
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated() {
            return g.V().has("person","name","marko").
                     mergeE(asMap(T.label, "knows")).
                       option(Merge.onCreate, asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100), "created", "Y")).
                       option(Merge.onMatch, asMap("created", "N"));
        }

        @Override
        public Traversal<Map<Object,Object>, Edge> get_g_injectXlabel_knows_out_marko_in_vadasX_mergeE() {
            return g.inject((Map<Object,Object>) asMap(T.label, "knows", Direction.IN, new ReferenceVertex(101), Direction.OUT, new ReferenceVertex(100))).mergeE();
        }
    }
}