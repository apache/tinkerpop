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
package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

/**
* @author Marko A. Rodriguez (http://markorodriguez.com)
*/
public final class ToyVertex implements Vertex, Vertex.Iterators, Serializable {

    private final Object id;
    private static final String TOY_VERTEX = "toyVertex";

    public ToyVertex(final Object id) {
        this.id = id;
    }

    ToyVertex() {
        this.id = null;
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return TOY_VERTEX;
    }

    @Override
    public Graph graph() {
        return EmptyGraph.instance();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Edge> edgeIterator(Direction direction, String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(final Object other) {
        return ElementHelper.areEqual(this, other);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
