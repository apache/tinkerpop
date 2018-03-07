/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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

import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sum;
import static org.junit.Assert.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class SumTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Number> get_g_V_age_sum();

    public abstract Traversal<Vertex, Number> get_g_V_age_fold_sumXlocalX();

    public abstract Traversal<Vertex, Number> get_g_V_foo_sum();

    public abstract Traversal<Vertex, Number> get_g_V_foo_fold_sumXlocalX();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_sum() {
        final Traversal<Vertex, Number> traversal = get_g_V_age_sum();
        printTraversalForm(traversal);
        final Number sum = traversal.next();
        assertEquals(123, sum.intValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_fold_sumXlocalX() {
        final Traversal<Vertex, Number> traversal = get_g_V_age_fold_sumXlocalX();
        printTraversalForm(traversal);
        final Number sum = traversal.next();
        assertEquals(123, sum.intValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_sum() {
        final Traversal<Vertex, Number> traversal = get_g_V_foo_sum();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_fold_sumXlocalX() {
        final Traversal<Vertex, Number> traversal = get_g_V_foo_fold_sumXlocalX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("ripple"));
        assertEquals(1.0, map.get("lop"));
    }

    public static class Traversals extends SumTest {

        @Override
        public Traversal<Vertex, Number> get_g_V_age_sum() {
            return g.V().values("age").sum();
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_age_fold_sumXlocalX() {
            return g.V().values("age").fold().sum(Scope.local);
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_foo_sum() {
            return g.V().values("foo").sum();
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_foo_fold_sumXlocalX() {
            return g.V().values("foo").fold().sum(Scope.local);
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_sumX() {
            return g.V().hasLabel("software").<String, Number>group().by("name").by(bothE().values("weight").sum());
        }
    }
}
