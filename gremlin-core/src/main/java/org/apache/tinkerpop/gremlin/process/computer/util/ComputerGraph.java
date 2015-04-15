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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerGraph implements Graph {

    public enum State {VERTEX_PROGRAM, MAP_REDUCE, NO_OP}

    private final Graph graph;
    private final Set<String> computeKeys;
    private State state = State.VERTEX_PROGRAM;

    public ComputerGraph(final Graph graph, final Set<String> elementComputeKeys) {
        this.graph = graph;
        this.computeKeys = elementComputeKeys;
    }

    public static Vertex of(final Vertex vertex, final Set<String> elementComputeKeys) {
        return new ComputerGraph(vertex.graph(), elementComputeKeys).wrapVertex(vertex);
    }

    private final Vertex wrapVertex(final Vertex vertex) {
        return new ComputerVertex(vertex);
    }

    public void setState(final State state) {
        this.state = state;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return new ComputerVertex(this.graph.addVertex(keyValues));
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return this.graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.graph.compute();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return IteratorUtils.map(this.graph.vertices(vertexIds), vertex -> new ComputerVertex(vertex));
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return IteratorUtils.map(this.graph.edges(edgeIds), edge -> new ComputerEdge(edge));
    }

    @Override
    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public Variables variables() {
        return this.graph.variables();
    }

    @Override
    public Configuration configuration() {
        return this.graph.configuration();
    }

    @Override
    public void close() throws Exception {
        this.graph.close();
    }

    private class ComputerElement implements Element {
        private final Element element;

        public ComputerElement(final Element element) {
            this.element = element;
        }

        @Override
        public Object id() {
            return this.element.id();
        }

        @Override
        public String label() {
            return this.element.label();
        }

        @Override
        public Graph graph() {
            return ComputerGraph.this;
        }

        @Override
        public Set<String> keys() {
            return this.element.keys().stream().filter(key -> !computeKeys.contains(key)).collect(Collectors.toSet());
        }

        @Override
        public <V> Property<V> property(final String key) {
            return new ComputerProperty<>(this.element.property(key));
        }

        @Override
        public <V> Property<V> property(final String key, final V value) {
            return new ComputerProperty<>(this.element.property(key, value));
        }

        @Override
        public <V> V value(final String key) throws NoSuchElementException {
            return this.element.value(key);
        }

        @Override
        public void remove() {
            this.element.remove();
        }

        @Override
        public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
            return (Iterator) IteratorUtils.filter(this.element.properties(propertyKeys), property -> !computeKeys.contains(property.key()));
        }

        @Override
        public <V> Iterator<V> values(final String... propertyKeys) {
            return IteratorUtils.map(this.<V>properties(propertyKeys), property -> property.value());
        }

        @Override
        public int hashCode() {
            return this.element.hashCode();
        }

        @Override
        public String toString() {
            return this.element.toString();
        }

        @Override
        public boolean equals(final Object other) {
            return ElementHelper.areEqual(this, other);
        }

        protected final Vertex asVertex() {
            return (Vertex) this.element;
        }

        protected final Edge asEdge() {
            return (Edge) this.element;
        }

        protected final <V> VertexProperty<V> asVertexProperty() {
            return (VertexProperty<V>) this.element;
        }
    }

    ///////////////////////////////////

    private class ComputerVertex extends ComputerElement implements Vertex {


        public ComputerVertex(final Vertex vertex) {
            super(vertex);
        }

        @Override
        public <V> VertexProperty<V> property(final String key) {
            return new ComputerVertexProperty<>(this.asVertex().property(key));
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value) {
            if(!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.asVertex().property(key, value));
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            if(!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.asVertex().property(key, value, keyValues));
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
            if(!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.asVertex().property(cardinality, key, value, keyValues));
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return new ComputerEdge(this.asVertex().addEdge(label, inVertex, keyValues));
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return IteratorUtils.map(this.asVertex().edges(direction, edgeLabels), edge -> new ComputerEdge(edge));
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return IteratorUtils.map(this.asVertex().vertices(direction, edgeLabels), vertex -> new ComputerVertex(vertex));
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerVertexProperty<V>((VertexProperty<V>) property));
        }
    }

    ////////////////////////////

    private class ComputerEdge extends ComputerElement implements Edge {

        public ComputerEdge(final Edge edge) {
            super(edge);
        }


        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            return IteratorUtils.map(this.asEdge().vertices(direction), vertex -> new ComputerVertex(vertex));
        }

        @Override
        public Vertex outVertex() {
            return new ComputerVertex(this.asEdge().outVertex());
        }

        @Override
        public Vertex inVertex() {
            return new ComputerVertex(this.asEdge().inVertex());
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerProperty(property));
        }
    }

    ///////////////////////////

    private class ComputerVertexProperty<V> extends ComputerElement implements VertexProperty<V> {
        public ComputerVertexProperty(final VertexProperty<V> vertexProperty) {
            super(vertexProperty);
        }

        @Override
        public String key() {
            return this.asVertexProperty().key();
        }

        @Override
        public V value() throws NoSuchElementException {
            return this.<V>asVertexProperty().value();
        }

        @Override
        public boolean isPresent() {
            return this.asVertexProperty().isPresent();
        }

        @Override
        public Vertex element() {
            return new ComputerVertex(this.asVertexProperty().element());
        }

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerProperty(property));
        }
    }

    ///////////////////////////

    private class ComputerProperty<V> implements Property<V> {

        private final Property<V> property;

        public ComputerProperty(final Property<V> property) {
            this.property = property;
        }

        @Override
        public String key() {
            return this.property.key();
        }

        @Override
        public V value() throws NoSuchElementException {
            return this.property.value();
        }

        @Override
        public boolean isPresent() {
            return this.property.isPresent();
        }

        @Override
        public Element element() {
            final Element element = this.property.element();
            if (element instanceof Vertex)
                return new ComputerVertex((Vertex) element);
            else if (element instanceof Edge)
                return new ComputerEdge((Edge) element);
            else
                return new ComputerVertexProperty((VertexProperty) element);
        }

        @Override
        public void remove() {
            this.property.remove();
        }
    }
}
