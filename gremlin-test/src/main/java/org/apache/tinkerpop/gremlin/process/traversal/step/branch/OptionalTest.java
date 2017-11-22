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
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Pieter Martin
 */
@RunWith(GremlinProcessRunner.class)
public abstract class OptionalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_optionalXoutXknowsXX(Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_optionalXinXknowsXX(Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path();

    public abstract Traversal<Vertex, Path> get_g_V_optionalXout_optionalXoutXX_path();

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_optionalXoutXknowsXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_optionalXoutXknowsXX(convertToVertexId(this.graph, "vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertex(this.graph, "vadas"), traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_optionalXinXknowsXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_optionalXinXknowsXX(convertToVertexId(this.graph, "vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertex(this.graph, "marko"), traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path();
        printTraversalForm(traversal);
        List<Path> paths = traversal.toList();
        assertEquals(6, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "josh")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "peter"))
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_optionalXout_optionalXoutXX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_optionalXout_optionalXoutXX_path();
        printTraversalForm(traversal);
        List<Path> paths = traversal.toList();
        assertEquals(10, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "josh")) && p.get(1).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "josh")) && p.get(1).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "peter")) && p.get(1).equals(convertToVertex(this.graph, "lop"))
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    public static class Traversals extends OptionalTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXoutXknowsXX(final Object v2Id) {
            return this.g.V(v2Id).optional(out("knows"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXinXknowsXX(final Object v2Id) {
            return this.g.V(v2Id).optional(in("knows"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path() {
            return this.g.V().hasLabel("person").optional(out("knows").optional(out("created"))).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_optionalXout_optionalXoutXX_path() {
            return this.g.V().optional(out().optional(out())).path();
        }

    }
}
