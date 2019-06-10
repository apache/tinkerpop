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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class LoopsTest extends AbstractGremlinProcessTest {

    /*
    g.V(1).repeat(both().simplePath()).until(has('name', 'peter').or().loops().is(3)).has('name', 'peter').path().by('name').
                forEachRemaining(System.out::println);
        System.out.println('--');
        g.V(v1Id).repeat(both().simplePath()).until(has('name', 'peter').or().loops().is(2)).has('name', 'peter').path().by('name').
                forEachRemaining(System.out::println);
        System.out.println('--');
        g.V(v1Id).repeat(both().simplePath()).until(has('name', 'peter').and().loops().is(3)).has('name', 'peter').path().by('name').
                forEachRemaining(System.out::println);
        System.out.println('--');
        g.V().emit(has('name', 'marko').or().loops().is(2)).repeat(out()).values('name').
                forEachRemaining(System.out::println);
     */
    public abstract Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX(final Object v1Id);

    public abstract Traversal<Vertex, String> get_g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        int paths = 0;
        boolean has3 = false, has4 = false;
        while (traversal.hasNext()) {
            final Path path = traversal.next();
            switch (path.size()) {
                case 3:
                    assertEquals("marko", path.get(0));
                    assertEquals("lop", path.get(1));
                    assertEquals("peter", path.get(2));
                    has3 = true;
                    break;
                case 4:
                    assertEquals("marko", path.get(0));
                    assertEquals("josh", path.get(1));
                    assertEquals("lop", path.get(2));
                    assertEquals("peter", path.get(3));
                    has4 = true;
                    break;
            }
            paths++;
        }
        assertTrue(has3 && has4);
        assertEquals(2, paths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Path path = traversal.next();
        assertEquals(3, path.size());
        assertEquals("marko", path.get(0));
        assertEquals("lop", path.get(1));
        assertEquals("peter", path.get(2));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX() {
        final Traversal<Vertex, Path> traversal = get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        final Path path = traversal.next();
        assertEquals(4, path.size());
        assertEquals("marko", path.get(0));
        assertEquals("josh", path.get(1));
        assertEquals("lop", path.get(2));
        assertEquals("peter", path.get(3));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
        final Traversal<Vertex, String> traversal = get_g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "ripple", "lop"), traversal);
    }

    public static class Traversals extends LoopsTest {

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX3XX_hasXname_peterX_path_byXnameX(final Object v1Id) {
            return g.V(v1Id).repeat(__.both().simplePath()).until(__.has("name", "peter").or().loops().is(3)).has("name", "peter").path().by("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_or_loops_isX2XX_hasXname_peterX_path_byXnameX(final Object v1Id) {
            return g.V(v1Id).repeat(__.both().simplePath()).until(__.has("name", "peter").or().loops().is(2)).has("name", "peter").path().by("name");
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX(final Object v1Id) {
            return g.V(v1Id).repeat(__.both().simplePath()).until(__.has("name", "peter").and().loops().is(3)).has("name", "peter").path().by("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
            return g.V().emit(__.has("name", "marko").or().loops().is(2)).repeat(__.out()).values("name");
        }
    }
}