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
package org.apache.tinkerpop.gremlin.tinkergraph.services;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;

/**
 * Count the IN/OUT/BOTH edges for a set of vertices. Demonstrates a {@link Service.Type#Streaming} service.
 */
public class TinkerDegreeCentralityFactory extends TinkerServiceRegistry.TinkerServiceFactory<Vertex,Long> implements Service<Vertex, Long> {

    public static final String NAME = "tinker.degree.centrality";

    public interface Params {
        /*
         * Specify the edge direction (optional), default is Direction.IN
         */
        String DIRECTION = "direction";

        Map DESCRIBE = asMap(
                Params.DIRECTION, "Specify the edge direction (optional), default is Direction.IN"
        );
    }

    public TinkerDegreeCentralityFactory(final TinkerGraph graph) {
        super(graph, NAME);
    }

    @Override
    public Type getType() {
        return Type.Streaming;
    }

    @Override
    public Map describeParams() {
        return Params.DESCRIBE;
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Streaming);
    }

    @Override
    public Service<Vertex, Long> createService(final boolean isStart, final Map params) {
        if (isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotStartTraversal);
        }
        return this;
    }

    @Override
    public CloseableIterator<Long> execute(final ServiceCallContext ctx, final Traverser.Admin<Vertex> in, final Map params) {
        final Direction direction = (Direction) params.getOrDefault(Params.DIRECTION, Direction.IN);

        final Vertex v = in.get();
        final long count;
        try (CloseableIterator<Edge> it = CloseableIterator.of(v.edges(direction))) {
            count = IteratorUtils.count(it);
        };

        return CloseableIterator.of(LongStream.range(0, in.bulk()).map(i -> count).iterator());
    }

    @Override
    public void close() {}
}



