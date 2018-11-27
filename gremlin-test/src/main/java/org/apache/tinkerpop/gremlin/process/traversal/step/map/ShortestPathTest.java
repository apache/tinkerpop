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
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathTestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgramTest.ALL_SHORTEST_PATHS;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPath.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ShortestPathTest extends AbstractGremlinProcessTest {

    private ShortestPathTestHelper helper;

    @Before
    public void initializeHelper() throws Exception {
        this.helper = new ShortestPathTestHelper(this, g);
    }

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath();

    public abstract Traversal<Vertex, Path> get_g_V_both_dedup_shortestPath();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_edgesIncluded();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_directionXINX();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_edgesXoutEX();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_edgesIncluded_edgesXoutEX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_targetXhasXname_markoXX();

    public abstract Traversal<Vertex, Path> get_g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_maxDistanceX1X();

    public abstract Traversal<Vertex, Path> get_g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS).map(helper::makePath)
                .collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_both_dedup_shortestPath() {
        final Traversal<Vertex, Path> traversal = get_g_V_both_dedup_shortestPath();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS).map(helper::makePath)
                .collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_edgesIncluded() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_edgesIncluded();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS).map(p -> helper.makePath(true, p))
                .collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_directionXINX() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_directionXINX();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> (p[0].equals("marko") && p.length == 1)
                        || (p[0].equals("vadas") && Arrays.asList("marko", "vadas").contains(p[p.length - 1]))
                        || (p[0].equals("lop") && Arrays.asList("marko", "lop", "josh", "peter").contains(p[p.length - 1]))
                        || (p[0].equals("josh") && Arrays.asList("marko", "josh").contains(p[p.length - 1]))
                        || (p[0].equals("ripple") && Arrays.asList("marko", "josh", "ripple").contains(p[p.length - 1]))
                        || (p[0].equals("peter") && p.length == 1))
                .map(helper::makePath).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_edgesXoutEX() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_edgesXoutEX();
        printTraversalForm(traversal);
        checkOutDirectedPaths(false, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_edgesIncluded_edgesXoutEX() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_edgesIncluded_edgesXoutEX();
        printTraversalForm(traversal);
        checkOutDirectedPaths(true, traversal);
    }

    private void checkOutDirectedPaths(final boolean includeEdges, final Traversal<Vertex, Path> traversal) {
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> (p[0].equals("marko") && !p[p.length - 1].equals("peter"))
                        || (p[0].equals("vadas") && p.length == 1)
                        || (p[0].equals("lop") && p.length == 1)
                        || (p[0].equals("josh") && Arrays.asList("lop", "josh", "ripple").contains(p[p.length - 1]))
                        || (p[0].equals("ripple") && p.length == 1)
                        || (p[0].equals("peter") && Arrays.asList("lop", "peter").contains(p[p.length - 1])))
                .map(names -> helper.makePath(includeEdges, names)).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX_shortestPath() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_markoX_shortestPath();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[0].equals("marko")).map(helper::makePath).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_targetXhasXname_markoXX() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_targetXhasXname_markoXX();
        printTraversalForm(traversal);
        checkPathsToMarko(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX() {
        final Traversal<Vertex, Path> traversal = get_g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX();
        printTraversalForm(traversal);
        checkPathsToMarko(traversal);
    }

    private void checkPathsToMarko(final Traversal<Vertex, Path> traversal) {
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[p.length - 1].equals("marko")).map(helper::makePath).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p ->
                        p[0].equals("marko") && Arrays.asList("lop", "ripple").contains(p[p.length - 1]))
                .map(helper::makePath).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX();
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(helper.makePath("marko", "lop", "josh"), traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.asList(
                helper.makePath("daniel", "gremlin", "stephen"),
                helper.makePath("daniel", "tinkergraph", "stephen"));
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.asList(
                helper.makePath("MIGHT AS WELL", "DRUMS", "MAYBE YOU KNOW HOW I FEEL"),
                helper.makePath("MIGHT AS WELL", "SHIP OF FOOLS", "MAYBE YOU KNOW HOW I FEEL"));
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX_shortestPath_maxDistanceX1X() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_markoX_shortestPath_maxDistanceX1X();
        printTraversalForm(traversal);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[0].equals("marko") && p.length <= 2).map(helper::makePath).collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X();
        printTraversalForm(traversal);
        final List<Path> expected = Stream.concat(Arrays.stream(ALL_SHORTEST_PATHS)
                        .filter(p -> p[0].equals("vadas") &&
                                Arrays.asList("vadas", "marko", "lop", "peter").contains(p[p.length - 1]))
                        .map(helper::makePath),
                Stream.of(helper.makePath("vadas", "marko", "lop", "josh")))
                .collect(Collectors.toList());
        checkResults(expected, traversal);
    }

    public static class Traversals extends ShortestPathTest {

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath() {
            return g.V().shortestPath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_both_dedup_shortestPath() {
            return g.V().both().dedup().shortestPath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_edgesIncluded() {
            return g.V().shortestPath().with(includeEdges);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_directionXINX() {
            return g.V().shortestPath().with(edges, Direction.IN);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_edgesXoutEX() {
            return g.V().shortestPath().with(edges, __.outE());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_edgesIncluded_edgesXoutEX() {
            return g.V().shortestPath().with(includeEdges).with(edges, __.outE());
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath() {
            return g.V().has("name", "marko").shortestPath();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_targetXhasXname_markoXX() {
            return g.V().shortestPath().with(target, __.has("name", "marko"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX() {
            return g.V().shortestPath().with(target, __.<Vertex, String>values("name").is("marko"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX() {
            return g.V().has("name", "marko").shortestPath().with(target, __.hasLabel("software"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX() {
            return g.V().has("name", "marko").shortestPath()
                    .with(target, __.has("name","josh"))
                    .with(distance, "weight");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX() {
            return g.V().has("name", "daniel").shortestPath()
                    .with(target, __.has("name","stephen"))
                    .with(edges, __.bothE("uses"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX() {
            return g.V().has("song", "name", "MIGHT AS WELL")
                    .shortestPath().
                            with(target, __.has("song", "name", "MAYBE YOU KNOW HOW I FEEL")).
                            with(edges, __.outE("followedBy")).
                            with(distance, "weight");
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_markoX_shortestPath_maxDistanceX1X() {
            return g.V().has("name", "marko").shortestPath()
                    .with(maxDistance, 1);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X() {
            return g.V().has("name", "vadas").shortestPath()
                    .with(distance, "weight")
                    .with(maxDistance, 1.3);
        }
    }
}