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
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SampleTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Edge, Edge> get_g_E_sampleX1X();

    public abstract Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX();

    public abstract Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX();

    public abstract Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX();

    public abstract Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_sampleX1X() {
        final Traversal<Edge, Edge> traversal = get_g_E_sampleX1X();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_sampleX2X_byXweightX() {
        final Traversal<Edge, Edge> traversal = get_g_E_sampleX2X_byXweightX();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertTrue(traversal.hasNext());
        traversal.next();
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_localXoutE_sampleX1X_byXweightXX() {
        final Traversal<Vertex, Edge> traversal = get_g_V_localXoutE_sampleX1X_byXweightXX();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            traversal.next();
        }
        assertEquals(3, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX() {
        final Traversal<Vertex, Map<String, Collection<Double>>> traversal =
                get_g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Collection<Double>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(2, map.get("software").size());
        assertEquals(2, map.get("person").size());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX() {
        final Traversal<Vertex, Map<String, Collection<Double>>> traversal =
                get_g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Collection<Double>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(4, map.get("software").size());
        assertEquals(5, map.get("person").size());
    }

    public static class Traversals extends SampleTest {
        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            return g.E().sample(1);
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            return g.E().sample(2).by("weight");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            return g.V().local(outE().sample(1).by("weight"));
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX() {
            return g.V().<String, Collection<Double>>group().by(T.label).by(bothE().values("weight").sample(2).fold());
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX() {
            return g.V().<String, Collection<Double>>group().by(T.label).by(bothE().values("weight").fold().sample(Scope.local, 5));
        }
    }
}