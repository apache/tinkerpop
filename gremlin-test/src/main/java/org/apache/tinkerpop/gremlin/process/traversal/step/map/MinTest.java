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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class MinTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Integer> get_g_V_age_min();

    public abstract Traversal<Vertex, Integer> get_g_V_age_fold_minXlocalX();

    public abstract Traversal<Vertex, Integer> get_g_V_aggregateXaX_byXageX_capXaX_minXlocalX();

    public abstract Traversal<Vertex, Integer> get_g_V_aggregateXaX_byXageX_capXaX_unfold_min();

    public abstract Traversal<Vertex, Comparable> get_g_V_aggregateXaX_byXfooX_capXaX_minXlocalX();

    public abstract Traversal<Vertex, Comparable> get_g_V_aggregateXaX_byXfooX_capXaX_unfold_min();

    public abstract Traversal<Vertex, Comparable> get_g_V_foo_min();

    public abstract Traversal<Vertex, Comparable> get_g_V_foo_fold_minXlocalX();

    public abstract Traversal<Vertex, String> get_g_V_name_min();

    public abstract Traversal<Vertex, String> get_g_V_name_fold_minXlocalX();

    public abstract Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min();

    public abstract Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX();

    public abstract Traversal<Vertex, Comparable> get_g_V_foo_injectX9999999999X_min();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_min() {
        final Traversal<Vertex, Integer> traversal = get_g_V_age_min();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_age_fold_minXlocalX() {
        final Traversal<Vertex, Integer> traversal = get_g_V_age_fold_minXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXaX_byXageX_capXaX_minXlocalX() {
        final Traversal<Vertex, Integer> traversal = get_g_V_aggregateXaX_byXageX_capXaX_minXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXaX_byXageX_capXaX_unfold_min() {
        final Traversal<Vertex, Integer> traversal = get_g_V_aggregateXaX_byXageX_capXaX_unfold_min();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_min() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_foo_min();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_fold_minXlocalX() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_foo_fold_minXlocalX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_min() {
        final Traversal<Vertex, String> traversal = get_g_V_name_min();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXaX_byXfooX_capXaX_minXlocalX() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_aggregateXaX_byXfooX_capXaX_minXlocalX();
        printTraversalForm(traversal);
        assertNull(traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_aggregateXaX_byXfooX_capXaX_unfold_min() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_aggregateXaX_byXfooX_capXaX_unfold_min();
        printTraversalForm(traversal);
        assertNull(traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_name_fold_minXlocalX() {
        final Traversal<Vertex, String> traversal = get_g_V_name_fold_minXlocalX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX5X_age_min() {
        final Traversal<Vertex, Integer> traversal = get_g_V_repeatXbothX_timesX5X_age_min();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(27), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX() {
        final Traversal<Vertex, Map<String, Number>> traversal = get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Number> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("ripple"));
        assertEquals(0.2, map.get("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_foo_injectX9999999999X_min() {
        final Traversal<Vertex, Comparable> traversal = get_g_V_foo_injectX9999999999X_min();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(9999999999L, traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            return g.V().values("age").min();
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_aggregateXaX_byXfooX_capXaX_minXlocalX() {
            return g.V().aggregate("a").by("foo").cap("a").min(Scope.local);
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_aggregateXaX_byXfooX_capXaX_unfold_min() {
            return g.V().aggregate("a").by("foo").cap("a").unfold().min();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_fold_minXlocalX() {
            return g.V().values("age").fold().min(Scope.local);
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_foo_min() {
            return g.V().values("foo").min();
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_foo_fold_minXlocalX() {
            return g.V().values("foo").fold().min(Scope.local);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_min() {
            return g.V().values("name").min();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_name_fold_minXlocalX() {
            return g.V().values("name").fold().min(Scope.local);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_aggregateXaX_byXageX_capXaX_minXlocalX() {
            return g.V().aggregate("a").by("age").cap("a").min(Scope.local);
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_aggregateXaX_byXageX_capXaX_unfold_min() {
            return g.V().aggregate("a").by("age").cap("a").unfold().min();
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            return g.V().repeat(both()).times(5).values("age").min();
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX() {
            return g.V().hasLabel("software").<String, Number>group().by("name").by(bothE().values("weight").min());
        }

        @Override
        public Traversal<Vertex, Comparable> get_g_V_foo_injectX9999999999X_min() {
            return g.V().values("foo").inject(9999999999L).min();
        }
    }
}