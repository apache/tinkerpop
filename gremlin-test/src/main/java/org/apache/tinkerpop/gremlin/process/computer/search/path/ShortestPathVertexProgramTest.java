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
package org.apache.tinkerpop.gremlin.process.computer.search.path;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class ShortestPathVertexProgramTest extends AbstractGremlinProcessTest {

    private ShortestPathTestHelper helper;

    @Before
    public void initializeHelper() throws Exception {
        this.helper  = new ShortestPathTestHelper(this, g);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindAllShortestPathsWithDefaultParameters() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build().create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS).map(helper::makePath).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindAllShortestPathsWithEdgesIncluded() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build().includeEdges(true).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS).map(p -> helper.makePath(true, p))
                .collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindOutDirectedShortestPaths() throws Exception {
        final List<ShortestPathVertexProgram> programs = Arrays.asList(
                ShortestPathVertexProgram.build().edgeTraversal(__.outE()).create(graph),
                ShortestPathVertexProgram.build().edgeDirection(Direction.OUT).create(graph));
        for (final ShortestPathVertexProgram program : programs) {
            final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                    program(program).submit().get();
            assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
            final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
            final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                    .filter(p -> (p[0].equals("marko") && !p[p.length - 1].equals("peter"))
                            || (p[0].equals("vadas") && p.length == 1)
                            || (p[0].equals("lop") && p.length == 1)
                            || (p[0].equals("josh") && Arrays.asList("lop", "josh", "ripple").contains(p[p.length - 1]))
                            || (p[0].equals("ripple") && p.length == 1)
                            || (p[0].equals("peter") && Arrays.asList("lop", "peter").contains(p[p.length - 1])))
                    .map(helper::makePath).collect(Collectors.toList());
            helper.checkResults(expected, shortestPaths);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindInDirectedShortestPaths() throws Exception {
        final List<ShortestPathVertexProgram> programs = Arrays.asList(
                ShortestPathVertexProgram.build().edgeTraversal(__.inE()).create(graph),
                ShortestPathVertexProgram.build().edgeDirection(Direction.IN).create(graph));
        for (final ShortestPathVertexProgram program : programs) {
            final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                    program(program).submit().get();
            assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
            final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
            final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                    .filter(p -> (p[0].equals("marko") && p.length == 1)
                            || (p[0].equals("vadas") && Arrays.asList("marko", "vadas").contains(p[p.length - 1]))
                            || (p[0].equals("lop") && Arrays.asList("marko", "lop", "josh", "peter").contains(p[p.length - 1]))
                            || (p[0].equals("josh") && Arrays.asList("marko", "josh").contains(p[p.length - 1]))
                            || (p[0].equals("ripple") && Arrays.asList("marko", "josh", "ripple").contains(p[p.length - 1]))
                            || (p[0].equals("peter") && p.length == 1))
                    .map(helper::makePath).collect(Collectors.toList());
            helper.checkResults(expected, shortestPaths);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindDirectedShortestPathsWithEdgesIncluded() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build().edgeTraversal(__.outE()).includeEdges(true).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> (p[0].equals("marko") && !p[p.length - 1].equals("peter"))
                        || (p[0].equals("vadas") && p.length == 1)
                        || (p[0].equals("lop") && p.length == 1)
                        || (p[0].equals("josh") && Arrays.asList("lop", "josh", "ripple").contains(p[p.length - 1]))
                        || (p[0].equals("ripple") && p.length == 1)
                        || (p[0].equals("peter") && Arrays.asList("lop", "peter").contains(p[p.length - 1])))
                .map(p -> helper.makePath(true, p)).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindShortestPathsWithStartVertexFilter() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build().source(__.has("name", "marko")).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[0].equals("marko")).map(helper::makePath).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindShortestPathsWithEndVertexFilter() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build().target(__.has("name", "marko")).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[p.length - 1].equals("marko")).map(helper::makePath).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFindShortestPathsWithStartEndVertexFilter() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .source(__.has("name", "marko"))
                        .target(__.hasLabel("software")).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p ->
                        p[0].equals("marko") && Arrays.asList("lop", "ripple").contains(p[p.length - 1]))
                .map(helper::makePath).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldUseCustomDistanceProperty() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .source(__.has("name", "marko"))
                        .target(__.has("name", "josh"))
                        .distanceProperty("weight").create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        assertEquals(1, shortestPaths.size());
        assertEquals(helper.makePath("marko", "lop", "josh"), shortestPaths.get(0));
    }

    @Test
    @LoadGraphWith(CREW)
    public void shouldFindEqualLengthPaths() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .edgeTraversal(__.bothE("uses"))
                        .source(__.has("name", "daniel"))
                        .target(__.has("name", "stephen")).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.asList(
                helper.makePath("daniel", "gremlin", "stephen"),
                helper.makePath("daniel", "tinkergraph", "stephen"));
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldFindEqualLengthPathsUsingDistanceProperty() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .edgeTraversal(__.outE("followedBy"))
                        .source(__.has("song", "name", "MIGHT AS WELL"))
                        .target(__.has("song", "name", "MAYBE YOU KNOW HOW I FEEL"))
                        .distanceProperty("weight")
                        .create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.asList(
                helper.makePath("MIGHT AS WELL", "DRUMS", "MAYBE YOU KNOW HOW I FEEL"),
                helper.makePath("MIGHT AS WELL", "SHIP OF FOOLS", "MAYBE YOU KNOW HOW I FEEL"));
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldRespectMaxDistance() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .source(__.has("name", "marko"))
                        .maxDistance(1).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Arrays.stream(ALL_SHORTEST_PATHS)
                .filter(p -> p[0].equals("marko") && p.length <= 2).map(helper::makePath).collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldRespectMaxCustomDistance() throws Exception {
        final ComputerResult result = graph.compute(graphProvider.getGraphComputer(graph).getClass()).
                program(ShortestPathVertexProgram.build()
                        .source(__.has("name", "vadas"))
                        .distanceProperty("weight").maxDistance(1.3).create(graph)).submit().get();
        assertTrue(result.memory().exists(ShortestPathVertexProgram.SHORTEST_PATHS));
        final List<Path> shortestPaths = result.memory().get(ShortestPathVertexProgram.SHORTEST_PATHS);
        final List<Path> expected = Stream.concat(Arrays.stream(ALL_SHORTEST_PATHS)
                        .filter(p -> p[0].equals("vadas") &&
                                Arrays.asList("vadas", "marko", "lop", "peter").contains(p[p.length - 1]))
                        .map(helper::makePath),
                Stream.of(helper.makePath("vadas", "marko", "lop", "josh")))
                .collect(Collectors.toList());
        helper.checkResults(expected, shortestPaths);
    }

    public static String[][] ALL_SHORTEST_PATHS = new String[][]{
            new String[]{"marko"},
            new String[]{"marko", "vadas"},
            new String[]{"marko", "lop"},
            new String[]{"marko", "lop", "peter"},
            new String[]{"marko", "josh"},
            new String[]{"marko", "josh", "ripple"},
            new String[]{"vadas"},
            new String[]{"vadas", "marko"},
            new String[]{"vadas", "marko", "lop"},
            new String[]{"vadas", "marko", "lop", "peter"},
            new String[]{"vadas", "marko", "josh", "ripple"},
            new String[]{"vadas", "marko", "josh"},
            new String[]{"lop"},
            new String[]{"lop", "marko"},
            new String[]{"lop", "marko", "vadas"},
            new String[]{"lop", "josh"},
            new String[]{"lop", "josh", "ripple"},
            new String[]{"lop", "peter"},
            new String[]{"josh"},
            new String[]{"josh", "marko"},
            new String[]{"josh", "marko", "vadas"},
            new String[]{"josh", "lop"},
            new String[]{"josh", "lop", "peter"},
            new String[]{"josh", "ripple"},
            new String[]{"ripple"},
            new String[]{"ripple", "josh"},
            new String[]{"ripple", "josh", "marko"},
            new String[]{"ripple", "josh", "marko", "vadas"},
            new String[]{"ripple", "josh", "lop"},
            new String[]{"ripple", "josh", "lop", "peter"},
            new String[]{"peter"},
            new String[]{"peter", "lop"},
            new String[]{"peter", "lop", "marko"},
            new String[]{"peter", "lop", "marko", "vadas"},
            new String[]{"peter", "lop", "josh"},
            new String[]{"peter", "lop", "josh", "ripple"}
    };
}