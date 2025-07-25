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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class InjectTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX();

    public abstract Traversal<Integer, Integer> get_g_injectXnull_nullX();

    public abstract Traversal<Integer, Integer> get_g_injectXnullX();

    public abstract Traversal<Integer, Integer> get_g_inject();

    public abstract Traversal<Vertex, Object> get_g_VX1X_valuesXageX_injectXnull_nullX(final Object vid1);

    public abstract Traversal<Vertex, Object> get_g_VX1X_valuesXageX_injectXnullX(final Object vid1);

    public abstract Traversal<Vertex, Object> get_g_VX1X_valuesXageX_inject(final Object vid1);

    public abstract Traversal<Integer, Map<String, Object>> get_g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX();

    public abstract Traversal<Map<String,Object>, Map<String, Object>> get_g_injectXname_marko_age_nullX_selectXname_ageX();

    public abstract Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX_asXaX_selectXaX();

    @Test
    public void g_injectXnull_1_3_nullX() {
        final Traversal<Integer, Integer> traversal = get_g_injectXnull_1_3_nullX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(null, 1, 3, null), traversal);
    }

    @Test
    public void g_injectXnull_nullX() {
        final Traversal<Integer, Integer> traversal = get_g_injectXnull_nullX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(null, null), traversal);
    }

    @Test
    public void g_injectXnullX() {
        final Traversal<Integer, Integer> traversal = get_g_injectXnullX();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(null), traversal);
    }

    @Test
    public void g_inject() {
        final Traversal<Integer, Integer> traversal = get_g_inject();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_valuesXageX_injectXnull_nullX() {
        final Traversal<Vertex, Object> traversal = get_g_VX1X_valuesXageX_injectXnull_nullX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29, null, null), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_valuesXageX_injectXnullX() {
        final Traversal<Vertex, Object> traversal = get_g_VX1X_valuesXageX_injectXnullX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29, null), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_valuesXageX_inject() {
        final Traversal<Vertex, Object> traversal = get_g_VX1X_valuesXageX_inject(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList(29), traversal);
    }

    @Test
    public void g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX() {
        final Traversal<Integer, Map<String, Object>> traversal = get_g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", 10, "b", 3L,
                    "a", 20, "b", 2L,
                    "a", null, "b", 1L), traversal);
    }

    @Test
    public void g_injectXname_marko_age_nullX_selectXname_ageX() {
        final Traversal<Map<String, Object>, Map<String, Object>> traversal = get_g_injectXname_marko_age_nullX_selectXname_ageX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2, "name", "marko", "age", null), traversal);
    }

    @Test
    public void g_injectXnull_1_3_nullX_asXaX_selectXaX() {
        final Traversal<Integer, Integer> traversal = get_g_injectXnull_1_3_nullX_asXaX_selectXaX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(null, 1, 3, null), traversal);
    }

    public static class Traversals extends InjectTest {

        @Override
        public Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX() {
            return g.inject(null, 1, 3, null);
        }

        @Override
        public Traversal<Integer, Integer> get_g_injectXnull_nullX() {
            return g.inject(null, null);
        }

        @Override
        public Traversal<Integer, Integer> get_g_injectXnullX() {
            return g.inject(null);
        }

        @Override
        public Traversal<Integer, Integer> get_g_inject() {
            return g.inject();
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_valuesXageX_injectXnull_nullX(final Object vid1) {
            return g.V(vid1).values("age").inject(null, null);
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_valuesXageX_injectXnullX(final Object vid1) {
            return g.V(vid1).values("age").inject(null);
        }

        @Override
        public Traversal<Vertex, Object> get_g_VX1X_valuesXageX_inject(final Object vid1) {
            return g.V(vid1).values("age").inject();
        }

        @Override
        public Traversal<Integer, Map<String, Object>> get_g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX() {
            return g.inject(10,20,null,20,10,10).groupCount("x").
                     dedup().as("y").
                     project("a","b").
                       by().
                       by(__.select("x").select(__.select("y")));
        }

        @Override
        public Traversal<Map<String, Object>, Map<String, Object>> get_g_injectXname_marko_age_nullX_selectXname_ageX() {
            final Map<String,Object> m = new HashMap<>();
            m.put("name", "marko");
            m.put("age", null);
            return g.inject(m).select("name","age");
        }

        @Override
        public Traversal<Integer, Integer> get_g_injectXnull_1_3_nullX_asXaX_selectXaX() {
            return g.inject(null, 1, 3, null).as("a").select("a");
        }
    }
}
