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
package org.apache.tinkerpop.gremlin.hadoop.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopEdge extends HadoopElement implements Edge, WrappedEdge<Edge> {

    protected HadoopEdge() {
    }

    public HadoopEdge(final Edge edge, final HadoopGraph graph) {
        super(edge, graph);
    }

    @Override
    public Edge getBaseEdge() {
        return (Edge) this.baseElement;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.graph.vertices(getBaseEdge().vertices(Direction.OUT).next().id())).next();
            case IN:
                return IteratorUtils.of(this.graph.vertices(getBaseEdge().vertices(Direction.IN).next().id())).next();
            default: {
                final Iterator<Vertex> iterator = getBaseEdge().vertices(Direction.BOTH);
                return IteratorUtils.of(this.graph.vertices(iterator.next().id()).next(), this.graph.vertices(iterator.next().id()).next());
            }
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return IteratorUtils.<Property<V>, Property<V>>map(this.getBaseEdge().properties(propertyKeys), property -> new HadoopProperty<>(property, HadoopEdge.this));
    }
}