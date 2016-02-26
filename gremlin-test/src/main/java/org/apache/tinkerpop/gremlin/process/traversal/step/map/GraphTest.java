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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class GraphTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_VX1X_V_valuesXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name();

    public abstract Traversal<Vertex, Edge> get_g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX();

    public abstract Traversal<Vertex, String> get_g_V_outXknowsX_V_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_V_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_V_valuesXnameX(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "lop", "josh", "ripple", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("MY BABE", "HOOCHIE COOCHIE MAN"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    public void g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            assertEquals("uses", edge.label());
            assertEquals("person", edge.outVertex().label());
            assertEquals("software", edge.inVertex().label());
            counter++;
        }
        assertEquals(8, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXknowsX_V_name() {
        final Traversal<Vertex, String> traversal = get_g_V_outXknowsX_V_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "marko", "josh", "josh", "vadas", "vadas", "peter", "peter", "lop", "lop", "ripple", "ripple"), traversal);
    }

    public static class Traversals extends GraphTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_V_valuesXnameX(final Object v1Id) {
            return g.V(v1Id).V().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_V_name() {
            return g.V().out("knows").V().values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name() {
            return g.V().has("artist", "name", "Garcia").in("sungBy").as("song")
                    .V().has("artist", "name", "Willie_Dixon").in("writtenBy").where(P.eq("song")).values("name");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX() {
            final List<Vertex> software = g.V().hasLabel("software").toList();
            return g.V().hasLabel("person").as("p").V(software).addE("uses").from("p");
        }
    }
}