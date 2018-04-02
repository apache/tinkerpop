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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MaxTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_age_max();

    public abstract Traversal<Vertex, Integer> get_g_V_age_fold_maxXlocalX();

    public abstract Traversal<Vertex, Comparable> get_g_V_foo_max();

    public abstract Traversal<Vertex, Comparable> get_g_V_foo_fold_maxXlocalX();

    public abstract Traversal<Vertex, String> get_g_V_name_max();

    public abstract Traversal<Vertex, String> get_g_V_name_fold_maxXlocalX();

    public abstract Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_max() {
        final Traversal<Vertex, Integer> traversal = get_g_V_age_max();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_fold_maxXlocalX() {
        final Traversal<Vertex, Integer> traversal = get_g_V_age_fold_maxXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_max() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_foo_max();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_fold_maxXlocalX() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_foo_fold_maxXlocalX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_max() {
        final Traversal<Vertex, String> traversal = get_g_V_name_max();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_fold_maxXlocalX() {
        final Traversal<Vertex, String> traversal = get_g_V_name_fold_maxXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX5X_age_max() {
        final Traversal<Vertex, Integer> traversal = get_g_V_repeatXbothX_timesX5X_age_max();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("ripple"));
        assertEquals(0.4, map.get("lop"));
    }

    public static class Traversals extends MaxTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_max() {
            return g.V().values("age").max();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_fold_maxXlocalX() {
            return g.V().values("age").fold().max(Scope.local);
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_foo_max() {
            return g.V().values("foo").max();
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_foo_fold_maxXlocalX() {
            return g.V().values("foo").fold().max(Scope.local);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_max() {
            return g.V().values("name").max();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_fold_maxXlocalX() {
            return g.V().values("name").fold().max(Scope.local);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_max() {
            return g.V().repeat(both()).times(5).values("age").max();
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_maxX() {
            return g.V().hasLabel("software").<String, Number>group().by("name").by(bothE().values("weight").max());
        }
    }
}
