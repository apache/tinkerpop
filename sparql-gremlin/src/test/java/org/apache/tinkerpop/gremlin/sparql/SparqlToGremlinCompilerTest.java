/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.sparql;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import static org.apache.tinkerpop.gremlin.sparql.SparqlToGremlinCompiler.convertToGremlinTraversal;
import static org.junit.Assert.assertEquals;


import java.io.IOException;

import static org.apache.tinkerpop.gremlin.sparql.ResourceHelper.loadQuery;


public class SparqlToGremlinCompilerTest {

    private Graph modern, crew;
    private GraphTraversalSource mg, cg;
    private GraphTraversalSource mc, cc;
/*
    @Before
    public void setUp() throws Exception {
        modern = TinkerFactory.createModern();
        mg = modern.traversal();
        mc = modern.traversal(computer());
        crew = TinkerFactory.createTheCrew();
        cg = modern.traversal();
        cc = modern.traversal(computer());
    }

    @Ignore
    @Test
    public void play() throws IOException {
        final String query = loadQuery("modern", 11);
        final Traversal traversal = convertToGremlinTraversal(modern, query);
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println(traversal);
    }

    /* Modern */

 /*   @Test
    public void testModern1() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30))).
                select("a", "b", "c");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 1)));
    }

    @Test
    public void testModern2() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30)));
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 2)));
    }

    @Test
    public void testModern3() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 3)));
    }

    @Test
    public void testModern4() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age"),
                as("person").out("created").as("project"),
                as("project").values("name").is("lop")).select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 4)));
    }

    @Test
    public void testModern5() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").is(29)).select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 5)));
    }

    @Test
    public void testModern6() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(and(as("age").is(gt(30)), as("age").is(lt(40)))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 6)));
    }

    @Test
    public void testModern7() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(or(as("age").is(lt(30)), as("age").is(gt(40)))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 7)));
    }

    @Test
    public void testModern8() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("person").values("name").as("name"),
                as("person").values("age").as("age")).where(
                or(and(as("age").is(gt(30)), as("age").is(lt(40))), as("name").is("marko"))).
                select("name", "age");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 8)));
    }

    @Test
    public void testModern9() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(as("a").values("age")).
                select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 9)));
    }

    @Test
    public void testModern10() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").values("name").as("name")).where(__.not(as("a").values("age"))).
                select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 10)));
    }

    @Test
    public void testModern11() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("name")).dedup("name").select("name");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 11)));
    }

    @Test
    public void testModern12() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("b").values("name").as("name")).dedup();
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 12)));
    }

    @Test
    public void testModern13() throws Exception {
        final GraphTraversal expected = mg.V().match(
                as("a").out("created").as("b"),
                as("a").values("name").as("c")).dedup("a", "b", "c").select("a", "b", "c");
        assertEquals(expected, convertToGremlinTraversal(modern, loadQuery("modern", 13)));
    }

    /* The Crew */

 /*   @Test
    public void testCrew1() throws Exception {
        final GraphTraversal expected = cg.V().match(
                as("a").values("name").is("daniel"),
                as("a").properties("location").as("b"),
                as("b").value().as("c"),
                as("b").values("startTime").as("d")).
                select("c", "d");
        assertEquals(expected, convertToGremlinTraversal(crew, loadQuery("crew", 1)));
    }

    /* Computer Mode */

  /*  @Test
    public void testModernInComputerMode() throws Exception {
        final GraphTraversal expected = mc.V().match(
                as("a").hasLabel("person"),
                as("a").out("knows").as("b"),
                as("a").out("created").as("c"),
                as("b").out("created").as("c"),
                as("a").values("age").as("d")).where(as("d").is(lt(30))).
                select("a", "b", "c");
        assertEquals(expected, convertToGremlinTraversal(mc, loadQuery("modern", 1)));
    }

    @Test
    public void testCrewInComputerMode() throws Exception {
        final GraphTraversal expected = cc.V().match(
                as("a").values("name").is("daniel"),
                as("a").properties("location").as("b"),
                as("b").value().as("c"),
                as("b").values("startTime").as("d")).
                select("c", "d");
        assertEquals(expected, convertToGremlinTraversal(crew, loadQuery("crew", 1)));
    } */
}