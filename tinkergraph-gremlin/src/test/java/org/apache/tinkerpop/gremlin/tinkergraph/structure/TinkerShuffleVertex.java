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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerShuffleGraph.shuffleIterator;

/**
 * @author Cole Greer (https://github.com/Cole-Greer)
 */
public final class TinkerShuffleVertex extends TinkerVertex {
    protected TinkerShuffleVertex(final Object id, final String label, final AbstractTinkerGraph graph) {
        super(id, label, graph);
    }

    protected TinkerShuffleVertex(final Object id, final String label, final AbstractTinkerGraph graph, final long currentVersion) {
        super(id, label, graph, currentVersion);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return shuffleIterator(super.edges(direction, edgeLabels));
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return shuffleIterator(super.vertices(direction, edgeLabels));
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return shuffleIterator(super.properties(propertyKeys));
    }

    @Override
    protected <V> TinkerVertexProperty<V> createTinkerVertexProperty(final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        return new TinkerShuffleVertexProperty<V>(vertex, key, value, propertyKeyValues);
    }

    @Override
    protected <V> TinkerVertexProperty<V> createTinkerVertexProperty(final Object id, final TinkerVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        return new TinkerShuffleVertexProperty<V>(id, vertex, key, value, propertyKeyValues);
    }
}
