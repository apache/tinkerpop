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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerShuffleGraph.shuffleIterator;

/**
 * @author Cole Greer (https://github.com/Cole-Greer)
 */
public final class TinkerShuffleEdge extends TinkerEdge {
    protected TinkerShuffleEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
       this(id, outVertex, label, inVertex, 0);
    }

    protected TinkerShuffleEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final long currentVersion) {
        super(id, outVertex, label, inVertex, currentVersion);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return shuffleIterator(super.vertices(direction));
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return shuffleIterator(super.properties(propertyKeys));
    }
}
