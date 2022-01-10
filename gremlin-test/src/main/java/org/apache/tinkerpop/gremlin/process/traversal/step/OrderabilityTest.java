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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(GremlinProcessRunner.class)
public abstract class OrderabilityTest extends AbstractGremlinProcessTest {

    private interface Constants {
        UUID uuid = UUID.randomUUID();
        Date date = new Date();
        List list1 = Arrays.asList(1, 2, 3);
        List list2 = Arrays.asList(1, 2, 3, 4);
        Set set1 = new LinkedHashSet(list1);
        Set set2 = new LinkedHashSet(list2);
        Map map1 = new LinkedHashMap() {{
            put(1, 11);
            put(2, 22);
            put(3, false);
            put(4, 44);
        }};
        Map map2 = new LinkedHashMap() {{
            put(1, 11);
            put(2, 22);
            put(3, 33);
        }};

        Object[] unordered = { map2, 1, map1, "foo", null, list1, date, set1, list2, true, uuid, "bar", 2.0, false, set2 };
    }

    public abstract Traversal<Vertex, Object> get_g_V_values_order();

    public abstract Traversal<Vertex, ? extends Property> get_g_V_properties_order();

    public abstract Traversal<Edge, Object> get_g_E_properties_order_value();

    public abstract Traversal<Edge, Object> get_g_E_properties_order_byXdescX_value();

    public abstract Traversal<Object, Object> get_g_inject_order();

    // order asc by vertex: v[3], v[5]
    public abstract Traversal<Vertex, Vertex> get_g_V_out_out_order_byXascX();

    // order asc by vertex in path: v[3], v[5]
    public abstract Traversal<Vertex, Vertex> get_g_V_out_out_asXheadX_path_order_byXascX_selectXheadX();

    // order asc by edge: e[10], v[e11]
    public abstract Traversal<Vertex, Edge> get_g_V_out_outE_order_byXascX();

    // order asc by edge in path: e[10], e[11]
    public abstract Traversal<Vertex, Edge> get_g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX();

    // order asc by vertex and then vertex property id in path.
    public abstract Traversal<Vertex, Object> get_g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value();

    // order asc by vertex and then vertex property value in path.
    public abstract Traversal<Vertex, Object> get_g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX();

    // order desc by vertex: v[3], v[5]
    public abstract Traversal<Vertex, Vertex> get_g_V_out_out_order_byXdescX();

    // order desc by vertex in path: v[3], v[5]
    public abstract Traversal<Vertex, Vertex> get_g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX();

    // order desc by edge: e[10], v[e11]
    public abstract Traversal<Vertex, Edge> get_g_V_out_outE_order_byXdescX();

    // order desc by edge in path: e[10], e[11]
    public abstract Traversal<Vertex, Edge> get_g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX();

    // order desc by vertex and then vertex property id in path.
    public abstract Traversal<Vertex, Object> get_g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value();

    // order desc by vertex and then vertex property value in path.
    public abstract Traversal<Vertex, Object> get_g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX();

    /**
     * Order by property value (mixed types).
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_values_order() {
        final Traversal<Vertex, Object> traversal = get_g_V_values_order();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                27, 29, 32, 35, "java", "java", "josh", "lop", "marko", "peter", "ripple", "vadas"
        ), traversal);
    }

    /**
     * Order by vertex property (orders by id).
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_properties_order() {
        final Traversal traversal = get_g_V_properties_order();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToVertexProperty("marko", "name", "marko"),    // vpid = 0
                convertToVertexProperty("marko", "age", 29),          // vpid = 1
                convertToVertexProperty("vadas", "name", "vadas"),    // vpid = 2
                convertToVertexProperty("vadas", "age", 27),          // vpid = 3
                convertToVertexProperty("lop", "name", "lop"),        // vpid = 4
                convertToVertexProperty("lop", "lang", "java"),       // vpid = 5
                convertToVertexProperty("josh", "name", "josh"),      // vpid = 6
                convertToVertexProperty("josh", "age", 32),           // vpid = 7
                convertToVertexProperty("ripple", "name", "ripple"),  // vpid = 8
                convertToVertexProperty("ripple", "lang", "java"),    // vpid = 9
                convertToVertexProperty("peter", "name", "peter"),    // vpid = 10
                convertToVertexProperty("peter", "age", 35)           // vpid = 11
        ), traversal);
    }

    /**
     * Order by edge property (orders by key, then value).
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_E_properties_order_value() {
        { // add some more edge properties
            final AtomicInteger a = new AtomicInteger();
            g.E().forEachRemaining(e -> e.property("a", a.getAndIncrement()));
        }

        final Traversal asc = get_g_E_properties_order_value();
        printTraversalForm(asc);
        checkOrderedResults(Arrays.asList(
                0, 1, 2, 3, 4, 5, 0.2, 0.4, 0.4, 0.5, 1.0, 1.0
        ), asc);

        final Traversal desc = get_g_E_properties_order_byXdescX_value();
        printTraversalForm(desc);
        checkOrderedResults(Arrays.asList(
                1.0, 1.0, 0.5, 0.4, 0.4, 0.2, 5, 4, 3, 2, 1, 0
        ), desc);
    }

    /**
     * Mixed type values including list, set, map, uuid, date, boolean, numeric, string, null.
     */
    @Test
    public void g_inject_order() {
        final Traversal traversal = get_g_inject_order();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                null,
                false, true,
                1, 2.0,
                Constants.date,
                "bar", "foo",
                Constants.uuid,
                Constants.set1, Constants.set2,
                Constants.list1, Constants.list2,
                Constants.map1, Constants.map2
        ), traversal);
    }

    /**
     * More mixed type values including a Java Object (unknown type).
     */
    @Test
    public void g_inject_order_with_unknown_type() {
        final Object unknown = new Object();
        final Object[] unordered = new Object[Constants.unordered.length+1];
        unordered[0] = unknown;
        System.arraycopy(Constants.unordered, 0, unordered, 1, Constants.unordered.length);

        final Traversal traversal = g.inject(unordered).order();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                null,
                false, true,
                1, 2.0,
                Constants.date,
                "bar", "foo",
                Constants.uuid,
                Constants.set1, Constants.set2,
                Constants.list1, Constants.list2,
                Constants.map1, Constants.map2,
                unknown
        ), traversal);
    }

    /**
     * Order asc by vertex: v[3], v[5]
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_order_byXascX() {
        final Traversal traversal = get_g_V_out_out_order_byXascX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToVertex("lop"),         // vid = 3
                convertToVertex("ripple")       // vid = 5
        ), traversal);
    }

    /**
     * Order asc by vertex in path: v[3], v[5]
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_asXheadX_path_order_byXascX_selectXheadX() {
        final Traversal traversal = get_g_V_out_out_asXheadX_path_order_byXascX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToVertex("lop"),         // vid = 3
                convertToVertex("ripple")       // vid = 5
        ), traversal);
    }

    /**
     * Order asc by edge: e[10], v[e11]
     *
     * Note to graph providers: if your graph does not support user-assigned edge ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_order_byXascX() {
        final Traversal traversal = get_g_V_out_outE_order_byXascX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToEdge("josh", "created", "ripple"),      // eid = 10
                convertToEdge("josh", "created", "lop")          // eid = 11
        ), traversal);
    }

    /**
     * Order asc by edge in path: e[10], e[11]
     *
     * Note to graph providers: if your graph does not support user-assigned edge ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX() {
        final Traversal traversal = get_g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToEdge("josh", "created", "ripple"),      // eid = 10
                convertToEdge("josh", "created", "lop")          // eid = 11
        ), traversal);
    }

    /**
     * Order asc by vertex and then vertex property id in path.
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids and vertex property ids you 
     * may need to skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value() {
        final Traversal traversal = get_g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                "lop",      // vid = 3, vpid = 4
                "java",     // vid = 3, vpid = 5
                "ripple",   // vid = 5, vpid = 8
                "java"      // vid = 5, vpid = 9
        ), traversal);
    }

    /**
     * Order asc by vertex and then vertex property value in path.
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX() {
        final Traversal traversal = get_g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                "java",     // vid = 3, val = "java"
                "lop",      // vid = 3, val = "lop"
                "java",     // vid = 5, val = "java"
                "ripple"    // vid = 5, val = "ripple"
        ), traversal);
    }

    /**
     * Order desc by vertex: v[5], v[3]
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_order_byXdescX() {
        final Traversal traversal = get_g_V_out_out_order_byXdescX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToVertex("ripple"),     // vid = 5
                convertToVertex("lop")         // vid = 3
        ), traversal);
    }

    /**
     * Order desc by vertex in path: v[5], v[3]
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX() {
        final Traversal traversal = get_g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToVertex("ripple"),     // vid = 5
                convertToVertex("lop")         // vid = 3
        ), traversal);
    }

    /**
     * Order desc by edge: e[11], v[e10]
     *
     * Note to graph providers: if your graph does not support user-assigned edge ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_order_byXdescX() {
        final Traversal traversal = get_g_V_out_outE_order_byXdescX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToEdge("josh", "created", "lop"),        // eid = 11
                convertToEdge("josh", "created", "ripple")      // eid = 10
        ), traversal);
    }

    /**
     * Order desc by edge in path: e[11], e[10]
     *
     * Note to graph providers: if your graph does not support user-assigned edge ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX() {
        final Traversal traversal = get_g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                convertToEdge("josh", "created", "lop"),        // eid = 11
                convertToEdge("josh", "created", "ripple")      // eid = 10
        ), traversal);
    }

    /**
     * Order desc by vertex and then vertex property id in path.
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids and vertex property ids you 
     * may need to skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value() {
        final Traversal traversal = get_g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                "java",     // vid = 5, vpid = 9
                "ripple",   // vid = 5, vpid = 8
                "java",     // vid = 3, vpid = 5
                "lop"       // vid = 3, vpid = 4
        ), traversal);
    }

    /**
     * Order desc by vertex and then vertex property value in path.
     *
     * Note to graph providers: if your graph does not support user-assigned vertex ids you may need to
     * skip this test.
     */
    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX() {
        final Traversal traversal = get_g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(
                "ripple",   // vid = 5, val = "ripple"
                "java",     // vid = 5, val = "java"
                "lop",      // vid = 3, val = "lop"
                "java"      // vid = 3, val = "java"
        ), traversal);
    }

    public static class Traversals extends OrderabilityTest implements Constants {

        @Override
        public Traversal<Vertex, Object> get_g_V_values_order() {
            return g.V().values().order();
        }

        @Override
        public Traversal<Vertex, ? extends Property> get_g_V_properties_order() {
            return g.V().properties().order();
        }

        @Override
        public Traversal<Edge, Object> get_g_E_properties_order_value() {
            return g.E().properties().order().value();
        }

        @Override
        public Traversal<Edge, Object> get_g_E_properties_order_byXdescX_value() {
            return g.E().properties().order().by(Order.desc).value();
        }

        @Override
        public Traversal<Object, Object> get_g_inject_order() {
            return g.inject(unordered).order();
        }

        // order asc by vertex: v[3], v[5]
        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_order_byXascX() {
            return g.V().out().out().order().by(Order.asc);
        }

        // order asc by vertex in path: v[3], v[5]
        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_asXheadX_path_order_byXascX_selectXheadX() {
            return g.V().out().out().as("head").path().order().by(Order.asc).select("head");
        }

        // order asc by edge: e[10], v[e11]
        @Override
        public Traversal<Vertex, Edge> get_g_V_out_outE_order_byXascX() {
            return g.V().out().outE().order().by(Order.asc);
        }

        // order asc by edge in path: e[10], e[11]
        @Override
        public Traversal<Vertex, Edge> get_g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX() {
            return g.V().out().outE().as("head").path().order().by(Order.asc).select("head");
        }

        // order asc by vertex and then vertex property id in path.
        @Override
        public Traversal<Vertex, Object> get_g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value() {
            return g.V().out().out().properties().as("head").path().order().by(Order.asc).select("head").value();
        }

        // order asc by vertex and then vertex property value in path.
        @Override
        public Traversal<Vertex, Object> get_g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX() {
            return g.V().out().out().values().as("head").path().order().by(Order.asc).select("head");
        }

        // order desc by vertex: v[5], v[3]
        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_order_byXdescX() {
            return g.V().out().out().order().by(Order.desc);
        }

        // order desc by vertex in path: v[5], v[3]
        @Override
        public Traversal<Vertex, Vertex> get_g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX() {
            return g.V().out().out().as("head").path().order().by(Order.desc).select("head");
        }

        // order desc by edge: e[11], v[e10]
        @Override
        public Traversal<Vertex, Edge> get_g_V_out_outE_order_byXdescX() {
            return g.V().out().outE().order().by(Order.desc);
        }

        // order desc by edge in path: e[11], e[10]
        @Override
        public Traversal<Vertex, Edge> get_g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX() {
            return g.V().out().outE().as("head").path().order().by(Order.desc).select("head");
        }

        // order desc by vertex and then vertex property id in path.
        @Override
        public Traversal<Vertex, Object> get_g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value() {
            return g.V().out().out().properties().as("head").path().order().by(Order.desc).select("head").value();
        }

        // order desc by vertex and then vertex property value in path.
        @Override
        public Traversal<Vertex, Object> get_g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX() {
            return g.V().out().out().values().as("head").path().order().by(Order.desc).select("head");
        }

    }
}
