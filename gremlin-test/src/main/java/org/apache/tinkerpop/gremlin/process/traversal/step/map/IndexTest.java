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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.tail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class IndexTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, List<Object>> get_g_V_hasLabelXsoftwareX_index_unfold();

    public abstract Traversal<Vertex, Map<Integer, Vertex>> get_g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX();

    public abstract Traversal<Vertex, List<Object>> get_g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX();

    public abstract Traversal<Vertex, Map<Integer, String>> get_g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_index() {
        final Traversal<Vertex, List<Object>> traversal = get_g_V_hasLabelXsoftwareX_index_unfold();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(
                Arrays.asList(convertToVertex("lop"), 0),
                Arrays.asList(convertToVertex("ripple"), 0)
        ), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX() {
        final Traversal<Vertex, Map<Integer, Vertex>> traversal = get_g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX();
        printTraversalForm(traversal);
        final Map<Integer, Vertex> map1 = new LinkedHashMap<>();
        final Map<Integer, Vertex> map2 = new LinkedHashMap<>();
        map1.put(0, convertToVertex("lop"));
        map2.put(0, convertToVertex("ripple"));
        checkOrderedResults(Arrays.asList(map1, map2), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX() {
        final Traversal<Vertex, List<Object>> traversal = get_g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                Arrays.asList("lop", 0),
                Arrays.asList("ripple", 1)), traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX() {
        final Traversal<Vertex, Map<Integer, String>> traversal = get_g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX();
        printTraversalForm(traversal);
        final Map<Integer, String> expected = new LinkedHashMap<>();
        expected.put(0, "josh");
        expected.put(1, "marko");
        expected.put(2, "peter");
        expected.put(3, "vadas");
        assertEquals(expected, traversal.next());
        assertFalse(traversal.hasNext());
    }

    public static class Traversals extends IndexTest {

        @Override
        public Traversal<Vertex, List<Object>> get_g_V_hasLabelXsoftwareX_index_unfold() {
            return g.V().hasLabel("software").index().unfold();
        }

        @Override
        public Traversal<Vertex, Map<Integer, Vertex>> get_g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX() {
            return g.V().hasLabel("software").order().by("name")
                    .<Map<Integer, Vertex>> index().with(WithOptions.indexer, WithOptions.map);
        }

        @Override
        public Traversal<Vertex, List<Object>> get_g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX() {
            return g.V().hasLabel("software").values("name").fold().order(Scope.local).index()
                    .<List<Object>> unfold().order().by(tail(Scope.local, 1));
        }

        @Override
        public Traversal<Vertex, Map<Integer, String>> get_g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX() {
            return g.V().hasLabel("person").values("name").fold().order(Scope.local)
                    .<Map<Integer, String>> index().with(WithOptions.indexer, WithOptions.map);
        }
    }
}
