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
import org.apache.tinkerpop.gremlin.process.*;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.*;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class GroupTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX();

    public abstract Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX();

    public abstract Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX();

    public abstract Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX();

    public abstract Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXnameX() {
        Arrays.asList(get_g_V_group_byXnameX(), get_g_V_groupXaX_byXnameX_capXaX()).forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Collection<Vertex>> map = traversal.next();
            assertEquals(6, map.size());
            map.forEach((key, values) -> {
                assertEquals(1, values.size());
                assertEquals(convertToVertexId(key), values.iterator().next().id());
            });
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
        final Traversal<Vertex, Map<String, Collection<String>>> traversal = get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX();
        printTraversalForm(traversal);
        final Map<String, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(2, map.get("java").size());
        assertTrue(map.get("java").contains("ripple"));
        assertTrue(map.get("java").contains("lop"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXlangX_group_byXlangX_byX1X_byXsizeX() {
        final Traversal<Vertex, Map<String, Long>> traversal = get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX();
        printTraversalForm(traversal);
        final Map<String, Long> map = traversal.next();
        assertEquals(1, map.size());
        assertTrue(map.containsKey("java"));
        assertEquals(Long.valueOf(2), map.get("java"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeatXout_groupXaX_byXnameX_byXitX_byXsizeXX_timesX2X_capXaX() {
        final List<Traversal<Vertex, Map<String, Long>>> traversals = new ArrayList<>();
        traversals.add(get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX());
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<String, Long> map = traversal.next();
            assertFalse(traversal.hasNext());
            assertEquals(4, map.size());
            assertTrue(map.containsKey("vadas"));
            assertEquals(Long.valueOf(1), map.get("vadas"));
            assertTrue(map.containsKey("josh"));
            assertEquals(Long.valueOf(1), map.get("josh"));
            assertTrue(map.containsKey("lop"));
            assertEquals(Long.valueOf(4), map.get("lop"));
            assertTrue(map.containsKey("ripple"));
            assertEquals(Long.valueOf(2), map.get("ripple"));
        });
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_group_byXoutE_countX_byXnameX() {
        final Traversal<Vertex, Map<Long, Collection<String>>> traversal = get_g_V_group_byXoutE_countX_byXnameX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Map<Long, Collection<String>> map = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(4, map.size());
        assertTrue(map.containsKey(0l));
        assertTrue(map.containsKey(1l));
        assertTrue(map.containsKey(2l));
        assertTrue(map.containsKey(3l));
        assertEquals(3, map.get(0l).size());
        assertEquals(1, map.get(1l).size());
        assertEquals(1, map.get(2l).size());
        assertEquals(1, map.get(3l).size());
        assertTrue(map.get(0l).contains("lop"));
        assertTrue(map.get(0l).contains("ripple"));
        assertTrue(map.get(0l).contains("vadas"));
        assertTrue(map.get(1l).contains("peter"));
        assertTrue(map.get(2l).contains("josh"));
        assertTrue(map.get(3l).contains("marko"));
    }

    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends GroupTest {

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_group_byXnameX() {
            return g.V().<String, Collection<Vertex>>group().by("name");
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<Vertex>>> get_g_V_groupXaX_byXnameX_capXaX() {
            return g.V().<String, Collection<Vertex>>group("a").by("name").cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Collection<String>>> get_g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX() {
            return g.V().has("lang").group("a").by("lang").by("name").out().cap("a");
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXlangX_group_byXlangX_byX1X_byXcountXlocalXX() {
            return g.V().has("lang").<String, Long>group().by("lang").by(inject(1)).<Collection>by(count(Scope.local));
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupXaX_byXnameX_by_byXcountXlocalXX_timesX2X_capXaX() {
            return g.V().repeat(out().group("a").by("name").by().<Collection>by(count(Scope.local))).times(2).cap("a");
        }

        @Override
        public Traversal<Vertex, Map<Long, Collection<String>>> get_g_V_group_byXoutE_countX_byXnameX() {
            return g.V().<Long, Collection<String>>group().by(outE().count()).by("name");
        }
    }
}
