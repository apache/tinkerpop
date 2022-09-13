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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;

/**
 * @author Valentyn Kahamlyk
 */
@RunWith(GremlinProcessRunner.class)
public abstract class EdgeTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Edge, Edge> get_g_EX11X_E(final Object e11Id);

    public abstract Traversal<Integer, Edge> get_g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_EX11X_E() {
        final Object edgeId = convertToEdgeId("josh", "created", "lop");
        final Traversal<Edge, Edge> traversal = get_g_EX11X_E(edgeId);
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(6, edges.size());
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX() {
        final Traversal<Integer, Edge> traversal = get_g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX();
        printTraversalForm(traversal);
        int counter = 0;
        final Set<Edge> edges = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            edges.add(traversal.next());
        }
        assertEquals(1, edges.size());
        assertEquals(1, counter);
    }

    public static class Traversals extends EdgeTest {

        @Override
        public Traversal<Edge, Edge> get_g_EX11X_E(final Object e11Id) {
            return g.E(e11Id).E();
        }

        @Override
        public Traversal<Integer, Edge> get_g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX() {
            return g.inject(1).coalesce(__.E().hasLabel("tests"), __.addE("tests").from(__.V().has("name","josh")).to(__.V().has("name","vadas")));
        }
    }
}