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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class RepeatTest extends AbstractGremlinProcessTest {

    // DO/WHILE


    public abstract Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path();

    public abstract Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X();

    public abstract Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit();

    // WHILE/DO

    public abstract Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path();

    public abstract Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path();

    public abstract Traversal<Vertex, String> get_g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id);

    // SIDE-EFFECTS

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX();

    //

    public abstract Traversal<Vertex, Map<String, Vertex>> get_g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX();

    public abstract Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXoutX_timesX2X_emit_path() {
        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXoutX_timesX2X_emit_path());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Integer, Long> pathLengths = new HashMap<>();
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                MapHelper.incr(pathLengths, traversal.next().size(), 1l);
            }
            assertEquals(2, pathLengths.size());
            assertEquals(8, counter);
            assertEquals(new Long(6), pathLengths.get(2));
            assertEquals(new Long(2), pathLengths.get(3));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "marko"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXoutX_timesX2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXoutX_timesX2X();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXoutX_timesX2X_emit() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_repeatXoutX_timesX2X_emit();
        printTraversalForm(traversal);
        final Map<String, Long> map = new HashMap<>();
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            MapHelper.incr(map, vertex.value("name"), 1l);
        }
        assertEquals(4, map.size());
        assertTrue(map.containsKey("vadas"));
        assertTrue(map.containsKey("josh"));
        assertTrue(map.containsKey("ripple"));
        assertTrue(map.containsKey("lop"));
        assertEquals(new Long(1), map.get("vadas"));
        assertEquals(new Long(1), map.get("josh"));
        assertEquals(new Long(2), map.get("ripple"));
        assertEquals(new Long(4), map.get("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_timesX2X_repeatXoutX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_timesX2X_repeatXoutX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_emit_timesX2X_repeatXoutX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_emit_timesX2X_repeatXoutX_path();
        printTraversalForm(traversal);
        assertPath(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_emit_repeatXoutX_timesX2X_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_emit_repeatXoutX_timesX2X_path();
        printTraversalForm(traversal);
        assertPath(traversal);
    }

    private static void assertPath(final Traversal<Vertex, Path> traversal) {
        int path1 = 0;
        int path2 = 0;
        int path3 = 0;
        while (traversal.hasNext()) {
            final Path path = traversal.next();
            if (path.size() == 1) {
                path1++;
            } else if (path.size() == 2) {
                path2++;
            } else if (path.size() == 3) {
                path3++;
            } else {
                fail("Only path lengths of 1, 2, or 3 should be seen");
            }
        }
        assertEquals(6, path1);
        assertEquals(6, path2);
        assertEquals(2, path3);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertFalse(traversal.hasNext());
        //[ripple:2, peter:1, vadas:2, josh:2, lop:4, marko:1]
        assertEquals(6, map.size());
        assertEquals(1l, map.get("marko").longValue());
        assertEquals(2l, map.get("vadas").longValue());
        assertEquals(2l, map.get("josh").longValue());
        assertEquals(4l, map.get("lop").longValue());
        assertEquals(2l, map.get("ripple").longValue());
        assertEquals(1l, map.get("peter").longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX() {
        final Traversal<Vertex, Map<String, Vertex>> traversal = get_g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Vertex> map = traversal.next();
            assertEquals(2, map.size());
            assertTrue(map.get("a") instanceof Vertex);
            assertTrue(map.get("b") instanceof Vertex);
            counter++;
        }
        assertTrue(counter > 0);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "lop", "ripple", "vadas"), traversal);
    }

    public static class Traversals extends RepeatTest {

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_emit_path() {
            return g.V().repeat(out()).times(2).emit().path();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name() {
            return g.V().repeat(out()).times(2).repeat(in()).times(2).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X() {
            return g.V().repeat(out()).times(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_repeatXoutX_timesX2X_emit() {
            return g.V().repeat(out()).times(2).emit();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_timesX2X_repeatXoutX_name(final Object v1Id) {
            return g.V(v1Id).times(2).repeat(out()).values("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_repeatXoutX_timesX2X_path() {
            return g.V().emit().repeat(out()).times(2).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_emit_timesX2X_repeatXoutX_path() {
            return g.V().emit().times(2).repeat(out()).path();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name(final Object v1Id) {
            return g.V(v1Id).emit(has(T.label, "person")).repeat(out()).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX() {
            return g.V().repeat(groupCount("m").by("name").out()).times(2).cap("m");
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX() {
            return g.V().repeat(both()).times(10).as("a").out().as("b").select("a", "b");
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name(final Object v1Id) {
            return g.V(v1Id).repeat(out()).until(outE().count().is(0)).values("name");
        }
    }
}