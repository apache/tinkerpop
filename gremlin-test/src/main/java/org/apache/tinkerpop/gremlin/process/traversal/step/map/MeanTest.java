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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MeanTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Double> get_g_V_age_mean();

    public abstract Traversal<Vertex, Double> get_g_V_age_fold_meanXlocalX();

    public abstract Traversal<Vertex, Number> get_g_V_foo_mean();

    public abstract Traversal<Vertex, Number> get_g_V_foo_fold_meanXlocalX();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_mean() {
        for (final Traversal<Vertex, Double> traversal : Arrays.asList(get_g_V_age_mean(), get_g_V_age_fold_meanXlocalX())) {
            printTraversalForm(traversal);
            final Double mean = traversal.next();
            assertEquals(30.75, mean, 0.05);
            assertFalse(traversal.hasNext());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_mean() {
        for (final Traversal<Vertex, Number> traversal : Arrays.asList(get_g_V_foo_mean(), get_g_V_foo_fold_meanXlocalX())) {
            printTraversalForm(traversal);
            assertFalse(traversal.hasNext());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("ripple"));
        assertEquals(1.0 / 3, map.get("lop"));
    }

    public static class Traversals extends MeanTest {

        @Override
        public Traversal<Vertex, Double> get_g_V_age_mean() {
            return g.V().values("age").mean();
        }

        @Override
        public Traversal<Vertex, Double> get_g_V_age_fold_meanXlocalX() {
            return g.V().values("age").fold().mean(Scope.local);
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_foo_mean() {
            return g.V().values("foo").mean();
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_foo_fold_meanXlocalX() {
            return g.V().values("foo").fold().mean(Scope.local);
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX() {
            return g.V().hasLabel("software").<String, Number>group().by("name").by(bothE().values("weight").mean());
        }
    }
}