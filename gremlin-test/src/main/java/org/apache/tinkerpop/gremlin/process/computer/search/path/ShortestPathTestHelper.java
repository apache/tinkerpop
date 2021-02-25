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

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class ShortestPathTestHelper {

    private final AbstractGremlinProcessTest test;
    private final GraphTraversalSource g;
    private final Map<String, Vertex> vertexCache;
    private final Map<Object, Map<Object, Edge>> edgeCache;

    public ShortestPathTestHelper(final AbstractGremlinProcessTest test, final GraphTraversalSource g) {
        this.test = test;
        this.g = g;
        this.vertexCache = new HashMap<>();
        this.edgeCache = new HashMap<>();
    }

    public void checkResults(final List<Path> expected, final List<Path> actual) {
        AbstractGremlinProcessTest.checkResults(expected, __.inject(actual.toArray(new Path[actual.size()])));
    }

    public Path makePath(final String... names) {
        return makePath(false, names);
    }

    public Path makePath(final boolean includeEdges, final String... names) {
        Path path = ImmutablePath.make();
        boolean first = true;
        for (final String name : names) {
            final Vertex vertex = vertexCache.computeIfAbsent(name, test::convertToVertex);
            if (!first) {
                if (includeEdges) {
                    final Object id1 = ((Vertex) path.get(path.size() - 1)).id();
                    final Object id2 = vertex.id();
                    final Edge edge;
                    if (edgeCache.containsKey(id1)) {
                        edge = edgeCache.get(id1).computeIfAbsent(id2, id -> getEdge(id1, id));
                    } else if (edgeCache.containsKey(id2)) {
                        edge = edgeCache.get(id2).computeIfAbsent(id1, id -> getEdge(id, id2));
                    } else {
                        edgeCache.put(id1, new HashMap<>());
                        edgeCache.get(id1).put(id2, edge = getEdge(id1, id2));
                    }
                    path = path.extend(edge, Collections.emptySet());
                }
            }
            path = path.extend(vertex, Collections.emptySet());
            first = false;
        }
        return path;
    }

    private Edge getEdge(final Object id1, final Object id2) {
        return g.V(id1)
                .bothE().filter(__.otherV().hasId(id2))
                .next();
    }
}
