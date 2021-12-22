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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    public abstract Traversal<Object, Object> get_g_inject_order();

    public abstract Traversal<Vertex, Path> get_g_V_out_out_valuesXnameX_path_order_byXascX();

    public abstract Traversal<Vertex, Path> get_g_V_out_out_valuesXnameX_path_order_byXdescX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_values_order() {
        final Traversal<Vertex, Object> traversal = get_g_V_values_order();
        printTraversalForm(traversal);
        checkOrderedResults(Arrays.asList(27, 29, 32, 35, "java", "java", "josh", "lop", "marko", "peter", "ripple", "vadas"), traversal);
    }

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

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_valuesXnameX_path_order_byXascX() {
        final List<Path> paths = Arrays.asList(
//                MutablePath.make().extend(convertToVertex("marko"), Collections.emptySet())
//                        .extend(convertToVertex("josh"), Collections.emptySet())
//                        .extend(convertToVertex("lop"), Collections.emptySet())
//                        .extend("lop", Collections.emptySet()),
//                MutablePath.make().extend(convertToVertex("marko"), Collections.emptySet())
//                        .extend(convertToVertex("josh"), Collections.emptySet())
//                        .extend(convertToVertex("ripple"), Collections.emptySet())
//                        .extend("ripple", Collections.emptySet())
        );

        final Traversal traversal = get_g_V_out_out_valuesXnameX_path_order_byXascX();
        printTraversalForm(traversal);
//        checkOrderedResults(paths, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_out_out_valuesXnameX_path_order_byXdescX() {
        final List<Path> paths = Arrays.asList(
                MutablePath.make().extend(convertToVertex("marko"), Collections.emptySet())
                        .extend(convertToVertex("josh"), Collections.emptySet())
                        .extend(convertToVertex("ripple"), Collections.emptySet())
                        .extend("ripple", Collections.emptySet()),
                MutablePath.make().extend(convertToVertex("marko"), Collections.emptySet())
                        .extend(convertToVertex("josh"), Collections.emptySet())
                        .extend(convertToVertex("lop"), Collections.emptySet())
                        .extend("lop", Collections.emptySet())
        );

        final Traversal traversal = get_g_V_out_out_valuesXnameX_path_order_byXdescX();
        printTraversalForm(traversal);
        checkOrderedResults(paths, traversal);
    }

    public static class Traversals extends OrderabilityTest implements Constants {

        @Override
        public Traversal<Vertex, Object> get_g_V_values_order() {
            return g.V().values().order();
        }

        @Override
        public Traversal<Object, Object> get_g_inject_order() {
            return g.inject(unordered).order();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_valuesXnameX_path_order_byXascX() {
            // [path[v[1], e[8][1-knows->4], v[4], e[10][4-created->5], v[5], vp[name->ripple], ripple], path[v[1], e[8][1-knows->4], v[4], e[11][4-created->3], v[3], vp[name->lop], lop]]
            return g.V().outE().inV().outE().inV().properties("name").value().path().order().by(Order.asc);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_valuesXnameX_path_order_byXdescX() {
            return g.V().out().out().values("name").path().order().by(Order.desc);
        }

    }
}
