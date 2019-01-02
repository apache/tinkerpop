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
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComputerGraph implements Graph {

    private enum State {VERTEX_PROGRAM, MAP_REDUCE}

    private ComputerVertex starVertex;
    private final Set<String> computeKeys;
    private State state;

    private ComputerGraph(final State state, final Vertex starVertex, final Optional<VertexProgram<?>> vertexProgram) {
        this.state = state;
        this.computeKeys = vertexProgram.isPresent() ? vertexProgram.get().getVertexComputeKeys().stream().map(VertexComputeKey::getKey).collect(Collectors.toSet()) : Collections.emptySet();
        this.starVertex = new ComputerVertex(starVertex);
    }

    public static ComputerVertex vertexProgram(final Vertex starVertex, VertexProgram vertexProgram) {
        return new ComputerGraph(State.VERTEX_PROGRAM, starVertex, Optional.of(vertexProgram)).starVertex;
    }

    public static ComputerVertex mapReduce(final Vertex starVertex) {
        return new ComputerGraph(State.MAP_REDUCE, starVertex, Optional.empty()).starVertex;
    }

    public ComputerVertex getStarVertex() {
        return this.starVertex;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction tx() {
        return this.starVertex.graph().tx();
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration configuration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException();
    }

    public class ComputerElement implements Element, WrappedElement<Element> {
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
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce();
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
            return this.element.equals(other);
        }

        @Override
        public Element getBaseElement() {
            return this.element;
        }
    }

    ///////////////////////////////////

    public class ComputerVertex extends ComputerElement implements Vertex, WrappedVertex<Vertex> {


        public ComputerVertex(final Vertex vertex) {
            super(vertex);
        }

        @Override
        public <V> VertexProperty<V> property(final String key) {
            return new ComputerVertexProperty<>(this.getBaseVertex().property(key));
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce();
            if (!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.getBaseVertex().property(key, value));
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce();
            if (!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.getBaseVertex().property(key, value, keyValues));
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce();
            if (!computeKeys.contains(key))
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            return new ComputerVertexProperty<>(this.getBaseVertex().property(cardinality, key, value, keyValues));
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return new ComputerEdge(this.getBaseVertex().addEdge(label, inVertex, keyValues));
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return IteratorUtils.map(this.getBaseVertex().edges(direction, edgeLabels), ComputerEdge::new);
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
            if (state.equals(State.MAP_REDUCE))
                throw GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce();
            return IteratorUtils.map(this.getBaseVertex().vertices(direction, edgeLabels), v -> v.equals(starVertex) ? starVertex : new ComputerAdjacentVertex(v));
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerVertexProperty<>((VertexProperty<V>) property));
        }

        @Override
        public Vertex getBaseVertex() {
            return (Vertex) this.getBaseElement();
        }
    }

    ////////////////////////////

    public class ComputerEdge extends ComputerElement implements Edge, WrappedEdge<Edge> {

        public ComputerEdge(final Edge edge) {
            super(edge);
        }


        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            if (direction.equals(Direction.OUT))
                return IteratorUtils.of(this.outVertex());
            if (direction.equals(Direction.IN))
                return IteratorUtils.of(this.inVertex());
            else
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }

        @Override
        public Vertex outVertex() {
            return this.getBaseEdge().outVertex().equals(starVertex) ? starVertex : new ComputerAdjacentVertex(this.getBaseEdge().outVertex());
        }

        @Override
        public Vertex inVertex() {
            return this.getBaseEdge().inVertex().equals(starVertex) ? starVertex : new ComputerAdjacentVertex(this.getBaseEdge().inVertex());
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerProperty(property));
        }

        @Override
        public Edge getBaseEdge() {
            return (Edge) this.getBaseElement();
        }
    }

    ///////////////////////////

    public class ComputerVertexProperty<V> extends ComputerElement implements VertexProperty<V>, WrappedVertexProperty<VertexProperty<V>> {
        public ComputerVertexProperty(final VertexProperty<V> vertexProperty) {
            super(vertexProperty);
        }

        @Override
        public String key() {
            return this.getBaseVertexProperty().key();
        }

        @Override
        public V value() throws NoSuchElementException {
            return this.<V>getBaseVertexProperty().value();
        }

        @Override
        public boolean isPresent() {
            return this.getBaseVertexProperty().isPresent();
        }

        @Override
        public Vertex element() {
            return new ComputerVertex(this.getBaseVertexProperty().element());
        }

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            return IteratorUtils.map(super.properties(propertyKeys), property -> new ComputerProperty(property));
        }

        @Override
        public VertexProperty<V> getBaseVertexProperty() {
            return (VertexProperty<V>) this.getBaseElement();
        }
    }

    ///////////////////////////

    public class ComputerProperty<V> implements Property<V>, WrappedProperty<Property<V>> {

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

        @Override
        public Property<V> getBaseProperty() {
            return this.property;
        }

        @Override
        public String toString() {
            return this.property.toString();
        }

        @Override
        public int hashCode() {
            return this.property.hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return this.property.equals(other);
        }
    }

    ///////////////////////////

    public class ComputerAdjacentVertex implements Vertex, WrappedVertex<Vertex> {

        private final Vertex adjacentVertex;

        public ComputerAdjacentVertex(final Vertex adjacentVertex) {
            this.adjacentVertex = adjacentVertex;
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
            throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
        }

        @Override
        public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
            throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
        }

        @Override
        public Object id() {
            return this.adjacentVertex.id();
        }

        @Override
        public String label() {
            throw GraphComputer.Exceptions.adjacentVertexLabelsCanNotBeRead();
        }

        @Override
        public Graph graph() {
            return ComputerGraph.this;
        }

        @Override
        public void remove() {

        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public int hashCode() {
            return this.adjacentVertex.hashCode();
        }

        @Override
        public String toString() {
            return this.adjacentVertex.toString();
        }

        @Override
        public boolean equals(final Object other) {
            return this.adjacentVertex.equals(other);
        }

        @Override
        public Vertex getBaseVertex() {
            return this.adjacentVertex;
        }
    }
}
