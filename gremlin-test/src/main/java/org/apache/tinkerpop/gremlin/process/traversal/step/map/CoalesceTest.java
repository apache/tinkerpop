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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class CoalesceTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX();

    public abstract Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX();

    public abstract Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX();

    public abstract Traversal<Vertex, String> get_g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutXfooX_outXbarXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_coalesceXoutXfooX_outXbarXX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals("lop", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<String, Long> result = traversal.next();
        assertEquals(4, result.size());
        assertTrue(result.containsKey("josh") && result.containsKey("lop") && result.containsKey("ripple") && result.containsKey("vadas"));
        assertEquals(1L, (long) result.get("josh"));
        assertEquals(2L, (long) result.get("lop"));
        assertEquals(1L, (long) result.get("ripple"));
        assertEquals(1L, (long) result.get("vadas"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
        final Traversal<Vertex, Path> traversal = get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX();
        printTraversalForm(traversal);
        final Map<String, Integer> first = new HashMap<>();
        final Map<String, Integer> last = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            first.compute(path.<String>get(0), (k, v) -> v != null ? v + 1 : 1);
            last.compute(path.<String>get(2), (k, v) -> v != null ? v + 1 : 1);
            assertEquals(3, path.size());
            assertTrue((path.<String>get(0).equals("marko") && path.<String>get(1).equals("knows") && path.<String>get(2).equals("vadas"))
                    || (path.<String>get(0).equals("marko") && path.<String>get(1).equals("knows") && path.<String>get(2).equals("josh"))
                    || (path.<String>get(0).equals("josh") && path.<String>get(1).equals("created") && path.<String>get(2).equals("lop"))
                    || (path.<String>get(0).equals("josh") && path.<String>get(1).equals("created") && path.<String>get(2).equals("ripple"))
                    || (path.<String>get(0).equals("peter") && path.<String>get(1).equals("created") && path.<String>get(2).equals("lop")));
        }
        assertEquals(5, counter);
        assertEquals(2, (int) first.get("marko"));
        assertEquals(2, (int) first.get("josh"));
        assertEquals(1, (int) first.get("peter"));
        assertEquals(1, (int) last.get("josh"));
        assertEquals(1, (int) last.get("vadas"));
        assertEquals(2, (int) last.get("lop"));
        assertEquals(1, (int) last.get("ripple"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX() {
        final Traversal<Vertex, String> traversal = get_g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "lop", "lop", "ripple"), traversal);
    }

    public static class Traversals extends CoalesceTest {
        @Override
        public Traversal<Vertex, Vertex> get_g_V_coalesceXoutXfooX_outXbarXX() {
            return g.V().coalesce(out("foo"), out("bar"));
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX(final Object v1Id) {
            return g.V(v1Id).coalesce(out("knows"), out("created")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX(final Object v1Id) {
            return g.V(v1Id).coalesce(out("created"), out("knows")).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX() {
            return g.V().coalesce(out("likes"), out("knows"), out("created")).<String>groupCount().by("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX() {
            return g.V().coalesce(outE("knows"), outE("created")).otherV().path().by("name").by(T.label);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX() {
            return g.V().out("created").order().by("name").coalesce(values("name"), constant("x"));
        }
    }
}
