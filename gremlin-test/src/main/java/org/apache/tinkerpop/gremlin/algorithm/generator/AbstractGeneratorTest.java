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
package org.apache.tinkerpop.gremlin.algorithm.generator;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Triplet;

import java.util.Iterator;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractGeneratorTest extends AbstractGremlinTest {

    /**
     * Asserts that two graphs are the "same" in way of structure.  It assumes that the graphs both have vertices
     * with an "oid" property that is an Integer value.  It iterates each vertex in graph 1, using the "oid" to
     * lookup vertices in graph 2.  For each one found it checks both IN and OUT vertices to ensure that they
     * attach to the same IN/OUT vertices given their "oid" properties.
     */
    protected boolean same(final Graph g1, final Graph g2) {
        final Iterator<Vertex> itty = g1.vertices();
        try {
            return IteratorUtils.stream(itty)
                    .map(v -> Triplet.<Integer, List<Vertex>, List<Vertex>>with(v.value("oid"), IteratorUtils.list(v.vertices(Direction.IN)), IteratorUtils.list(v.vertices(Direction.OUT))))
                    .allMatch(p -> {
                        final Iterator<Vertex> innerItty = g2.vertices();
                        final Vertex v = IteratorUtils.filter(innerItty, vx -> vx.value("oid").equals(p.getValue0())).next();
                        CloseableIterator.closeIterator(innerItty);
                        return sameInVertices(v, p.getValue1()) && sameOutVertices(v, p.getValue2());
                    });
        } finally {
            CloseableIterator.closeIterator(itty);
        }
    }

    private boolean sameInVertices(final Vertex v, final List<Vertex> inVertices) {
        return inVertices.stream()
                .allMatch(inVertex -> IteratorUtils.filter(v.vertices(Direction.IN), hv -> hv.value("oid").equals(inVertex.value("oid"))).hasNext());
    }

    private boolean sameOutVertices(final Vertex v, final List<Vertex> outVertices) {
        return outVertices.stream()
                .allMatch(outVertex -> IteratorUtils.filter(v.vertices(Direction.OUT), hv -> hv.value("oid").equals(outVertex.value("oid"))).hasNext());
    }
}
