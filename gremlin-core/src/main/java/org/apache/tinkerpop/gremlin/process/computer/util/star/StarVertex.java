/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.computer.util.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarVertex extends StarElement implements Vertex {

    protected Map<String, List<VertexProperty<?>>> properties = null;
    protected Map<String, List<Edge>> outEdges = new HashMap<>();
    protected Map<String, List<Edge>> inEdges = new HashMap<>();

    public StarVertex(final Object id, final String label) {
        super(id, label);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        List<Edge> outE = this.outEdges.get(label);
        if (null == outE) {
            outE = new ArrayList<>();
            this.outEdges.put(label, outE);
        }
        final StarOutEdge outEdge = new StarOutEdge(ElementHelper.getIdValue(keyValues).orElse(null), this, label, (StarInVertex) inVertex);
        outE.add(outEdge);
        return outEdge;
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, V value, final Object... keyValues) {
        if (null == this.properties)
            this.properties = new HashMap<>();

        final List<VertexProperty<?>> list = cardinality.equals(VertexProperty.Cardinality.single) ? new ArrayList<>(1) : this.properties.getOrDefault(key, new ArrayList<>());
        final VertexProperty<V> vertexProperty = new StarVertexProperty<>(ElementHelper.getIdValue(keyValues).orElse(null), key, value, this, keyValues);
        list.add(vertexProperty);
        this.properties.put(key, list);
        return vertexProperty;
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        if (direction.equals(Direction.OUT)) {
            return this.outEdges.entrySet().stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                    .map(Map.Entry::getValue)
                    .flatMap(List::stream)
                    .iterator();
        } else if (direction.equals(Direction.IN)) {
            return this.inEdges.entrySet().stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                    .map(Map.Entry::getValue)
                    .flatMap(List::stream)
                    .iterator();
        } else {
            return Stream.concat(this.inEdges.entrySet().stream(), this.outEdges.entrySet().stream())
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                    .map(Map.Entry::getValue)
                    .flatMap(List::stream)
                    .iterator();
        }
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        if (direction.equals(Direction.OUT)) {
            return this.outEdges.entrySet().stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                    .map(Map.Entry::getValue)
                    .flatMap(List::stream)
                    .map(Edge::inVertex)
                    .iterator();
        } else if (direction.equals(Direction.IN)) {
            return this.inEdges.entrySet().stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                    .map(Map.Entry::getValue)
                    .flatMap(List::stream)
                    .map(Edge::outVertex)
                    .iterator();
        } else {
            return null;
        }
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return null == this.properties ?
                Collections.emptyIterator() :
                (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).iterator();
    }
}
