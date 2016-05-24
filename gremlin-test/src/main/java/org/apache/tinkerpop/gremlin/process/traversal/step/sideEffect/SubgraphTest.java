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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SubgraphTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph);

    public abstract Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph);

    public abstract Traversal<Vertex, Vertex> get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(final Graph subgraph);

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName(), MODERN);
        graphProvider.clear(config);
        Graph subgraph = graphProvider.openTestGraph(config);
        /////
        final Traversal<Vertex, Graph> traversal = get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(convertToVertexId("marko"), subgraph);
        printTraversalForm(traversal);
        subgraph = traversal.next();
        assertVertexEdgeCounts(subgraph, 3, 2);
        subgraph.edges().forEachRemaining(e -> {
            assertEquals("knows", e.label());
            assertEquals("marko", e.outVertex().values("name").next());
            assertEquals(new Integer(29), e.outVertex().<Integer>values("age").next());
            assertEquals("person", e.outVertex().label());

            final String name = e.inVertex().<String>values("name").next();
            if (name.equals("vadas"))
                assertEquals(0.5d, e.value("weight"), 0.0001d);
            else if (name.equals("josh"))
                assertEquals(1.0d, e.value("weight"), 0.0001d);
            else
                fail("There's a vertex present that should not be in the subgraph");
        });
        graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName(), MODERN);
        graphProvider.clear(config);
        Graph subgraph = graphProvider.openTestGraph(config);
        /////
        final Traversal<Vertex, String> traversal = get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(subgraph);
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
        subgraph = traversal.asAdmin().getSideEffects().<Graph>get("sg");
        assertVertexEdgeCounts(subgraph, 5, 4);

        graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(CREW)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void g_withSideEffectXsgX_V_hasXname_danielXout_capXsgX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName(), CREW);
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        /////
        final Traversal<Vertex, Vertex> traversal = get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(subgraph);
        printTraversalForm(traversal);
        traversal.iterate();
        assertVertexEdgeCounts(subgraph, 3, 2);

        final List<String> locations = subgraph.traversal().V().has("name", "daniel").<String>values("location").toList();
        assertThat(locations, contains("spremberg", "kaiserslautern", "aachen"));

        graphProvider.clear(subgraph, config);
    }

    public static class Traversals extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph) {
            return g.withSideEffect("sg", () -> subgraph).V(v1Id).outE("knows").subgraph("sg").values("name").cap("sg");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph) {
            return g.withSideEffect("sg", () -> subgraph).V().repeat(bothE("created").subgraph("sg").outV()).times(5).<String>values("name").dedup();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_withSideEffectXsgX_V_hasXname_danielX_outE_subgraphXsgX_inV(final Graph subgraph) {
            return g.withSideEffect("sg", () -> subgraph).V().has("name","daniel").outE().subgraph("sg").inV();
        }
    }
}
