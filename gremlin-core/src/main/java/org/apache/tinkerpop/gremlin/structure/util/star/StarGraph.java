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

package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StarGraph implements Graph {

    private static final Configuration STAR_GRAPH_CONFIGURATION = new BaseConfiguration();

    static {
        STAR_GRAPH_CONFIGURATION.setProperty(Graph.GRAPH, StarGraph.class.getCanonicalName());
    }

    private StarVertex starVertex = null;
    private Long nextId = 0l;

    private final Map<Object, Map<String, Object>> edgeProperties = new HashMap<>();
    private final Map<Object, Map<String, Object>> metaProperties = new HashMap<>();

    public StarVertex getStarVertex() {
        return this.starVertex;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        return null == this.starVertex ?
                this.starVertex = new StarVertex(ElementHelper.getIdValue(keyValues).get(), ElementHelper.getLabelValue(keyValues).get()) :
                new StarAdjacentVertex(ElementHelper.getIdValue(keyValues).get());
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (null == this.starVertex)
            return Collections.emptyIterator();
        else if (vertexIds.length > 0 && vertexIds[0] instanceof StarVertex)
            return Stream.of(vertexIds).map(v -> (Vertex) v).iterator();  // todo: maybe do this better - not sure of star semantics here
        else if (ElementHelper.idExists(this.starVertex.id(), vertexIds))
            return IteratorUtils.of(this.starVertex);
        else
            return Collections.emptyIterator();
        // TODO: is this the semantics we want? the only "real vertex" is star vertex.
        /*return null == this.starVertex ?
                Collections.emptyIterator() :
                Stream.concat(
                        Stream.of(this.starVertex),
                        Stream.concat(
                                this.starVertex.outEdges.values()
                                        .stream()
                                        .flatMap(List::stream)
                                        .map(Edge::inVertex),
                                this.starVertex.inEdges.values()
                                        .stream()
                                        .flatMap(List::stream)
                                        .map(Edge::outVertex)))
                        .filter(vertex -> ElementHelper.idExists(vertex.id(), vertexIds))
                        .iterator();*/
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return null == this.starVertex ?
                Collections.emptyIterator() :
                Stream.concat(
                        this.starVertex.inEdges.values().stream(),
                        this.starVertex.outEdges.values().stream())
                        .flatMap(List::stream)
                        .filter(edge -> {
                            // todo: kinda fishy - need to better nail down how stuff should work here - none of these feel consistent right now.
                            if (edgeIds.length > 0 && edgeIds[0] instanceof Edge)
                                return ElementHelper.idExists(edge.id(), Stream.of(edgeIds).map(e -> ((Edge) e).id()).toArray());
                            else
                                return ElementHelper.idExists(edge.id(), edgeIds);
                        })
                        .iterator();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return STAR_GRAPH_CONFIGURATION;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "starOf:" + this.starVertex);
    }

    public static StarGraph open() {
        return new StarGraph();
    }

    public static StarGraph of(final Vertex vertex) {
        final StarGraph starGraph = new StarGraph();
        StarGraph.addTo(starGraph, DetachedFactory.detach(vertex, true));
        vertex.edges(Direction.BOTH).forEachRemaining(edge -> StarGraph.addTo(starGraph, DetachedFactory.detach(edge, true)));
        return starGraph;
    }

    public static Vertex addTo(final StarGraph graph, final DetachedVertex detachedVertex) {
        if (null != graph.starVertex)
            return null;
        graph.addVertex(T.id, detachedVertex.id(), T.label, detachedVertex.label());
        detachedVertex.properties().forEachRemaining(detachedVertexProperty -> {
            final VertexProperty<?> starVertexProperty = graph.starVertex.property(VertexProperty.Cardinality.list, detachedVertexProperty.key(), detachedVertexProperty.value(), T.id, detachedVertexProperty.id());
            detachedVertexProperty.properties().forEachRemaining(detachedVertexPropertyProperty -> starVertexProperty.property(detachedVertexPropertyProperty.key(), detachedVertexPropertyProperty.value()));
        });
        return graph.starVertex;
    }

    public static Edge addTo(final StarGraph graph, final DetachedEdge edge) {
        final Edge starEdge = !graph.starVertex.id().equals(edge.inVertex().id()) ?
                graph.starVertex.addOutEdge(edge.label(), edge.inVertex(), T.id, edge.id()) :
                graph.starVertex.addInEdge(edge.label(), edge.outVertex(), T.id, edge.id());
        edge.properties().forEachRemaining(property -> starEdge.property(property.key(), property.value()));
        return starEdge;
    }

    protected Long generateId() {
        return this.nextId++;
    }

    ///////////////////////
    //// STAR ELEMENT ////
    //////////////////////

    public abstract class StarElement implements Element {

        protected final Object id;
        protected final String label;

        protected StarElement(final Object id, final String label) {
            this.id = id;
            this.label = label.intern();
        }

        @Override
        public Object id() {
            return this.id;
        }

        @Override
        public String label() {
            return this.label;
        }

        @Override
        public Graph graph() {
            return StarGraph.this;
        }

        @Override
        public boolean equals(final Object other) {
            return ElementHelper.areEqual(this, other);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode(this);
        }
    }

    //////////////////////
    //// STAR VERTEX ////
    /////////////////////

    public final class StarVertex extends StarElement implements Vertex {

        private Map<String, List<Edge>> outEdges = new HashMap<>();
        private Map<String, List<Edge>> inEdges = new HashMap<>();
        private Map<String, List<VertexProperty>> vertexProperties = new HashMap<>();

        public StarVertex(final Object id, final String label) {
            super(id, label);
        }

        public void dropEdges() {
            this.outEdges.clear();
            this.inEdges.clear();
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            return this.addOutEdge(label, inVertex, keyValues);
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            return this.property(VertexProperty.Cardinality.single, key, value, keyValues);
        }

        protected Edge addOutEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            List<Edge> outE = this.outEdges.get(label);
            if (null == outE) {
                outE = new ArrayList<>();
                this.outEdges.put(label, outE);
            }
            final StarEdge outEdge = new StarOutEdge(ElementHelper.getIdValue(keyValues).orElse(generateId()), label, inVertex.id());
            ElementHelper.attachProperties(outEdge, keyValues);
            outE.add(outEdge);
            return outEdge;
        }

        protected Edge addInEdge(final String label, final Vertex outVertex, final Object... keyValues) {
            List<Edge> inE = this.inEdges.get(label);
            if (null == inE) {
                inE = new ArrayList<>();
                this.inEdges.put(label, inE);
            }
            final StarEdge inEdge = new StarInEdge(ElementHelper.getIdValue(keyValues).orElse(generateId()), label, outVertex.id());
            ElementHelper.attachProperties(inEdge, keyValues);
            inE.add(inEdge);
            return inEdge;
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, V value, final Object... keyValues) {
            final List<VertexProperty> list = cardinality.equals(VertexProperty.Cardinality.single) ? new ArrayList<>(1) : this.vertexProperties.getOrDefault(key, new ArrayList<>());
            final VertexProperty<V> vertexProperty = new StarVertexProperty<>(ElementHelper.getIdValue(keyValues).orElse(generateId()), key, value);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            list.add(vertexProperty);
            this.vertexProperties.put(key, list);
            return vertexProperty;
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            if (direction.equals(Direction.OUT)) {
                return edgeLabels.length == 0 ?
                        IteratorUtils.flatMap(this.outEdges.values().iterator(), List::iterator) :
                        this.outEdges.entrySet().stream()
                                .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                                .map(Map.Entry::getValue)
                                .flatMap(List::stream)
                                .iterator();
            } else if (direction.equals(Direction.IN)) {
                return edgeLabels.length == 0 ?
                        IteratorUtils.flatMap(this.inEdges.values().iterator(), List::iterator) :
                        this.inEdges.entrySet().stream()
                                .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                                .map(Map.Entry::getValue)
                                .flatMap(List::stream)
                                .iterator();
            } else
                return IteratorUtils.concat(this.edges(Direction.IN, edgeLabels), this.edges(Direction.OUT, edgeLabels));
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
            if (direction.equals(Direction.OUT))
                return IteratorUtils.map(this.edges(direction, edgeLabels), Edge::inVertex);
            else if (direction.equals(Direction.IN))
                return IteratorUtils.map(this.edges(direction, edgeLabels), Edge::outVertex);
            else
                return IteratorUtils.concat(this.vertices(Direction.IN, edgeLabels), this.vertices(Direction.OUT, edgeLabels));
        }

        @Override
        public void remove() {
            throw new IllegalStateException("The star vertex can not be removed from the StarGraph: " + this);
        }

        @Override
        public String toString() {
            return StringFactory.vertexString(this);
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
            if (this.vertexProperties.isEmpty())
                return Collections.emptyIterator();
            else if (propertyKeys.length == 0)
                return (Iterator) this.vertexProperties.entrySet().stream()
                        .flatMap(entry -> entry.getValue().stream())
                        .iterator();
            else if (propertyKeys.length == 1)
                return (Iterator) this.vertexProperties.getOrDefault(propertyKeys[0], Collections.emptyList()).iterator();
            else
                return (Iterator) this.vertexProperties.entrySet().stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .flatMap(entry -> entry.getValue().stream())
                        .iterator();
        }
    }

    ///////////////////////////////
    //// STAR VERTEX PROPERTY ////
    //////////////////////////////

    public final class StarVertexProperty<V> extends StarElement implements VertexProperty<V> {

        private final V value;

        public StarVertexProperty(final Object id, final String key, final V value) {
            super(id, key);
            this.value = value;
        }

        @Override
        public String key() {
            return this.label;
        }

        @Override
        public V value() throws NoSuchElementException {
            return this.value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public Vertex element() {
            return starVertex;
        }

        @Override
        public void remove() {
            StarGraph.this.starVertex.vertexProperties.get(this.label).remove(this);
        }

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            final Map<String, Object> properties = metaProperties.get(this.id);
            if (null == properties || properties.isEmpty())
                return Collections.emptyIterator();
            else if (propertyKeys.length == 0)
                return (Iterator) properties.entrySet().stream()
                        .map(entry -> new StarProperty<>(entry.getKey(), entry.getValue(), this))
                        .iterator();
            else if (propertyKeys.length == 1) {
                final Object v = properties.get(propertyKeys[0]);
                return null == v ?
                        Collections.emptyIterator() :
                        (Iterator) IteratorUtils.of(new StarProperty<>(propertyKeys[0], v, this));
            } else {
                return (Iterator) properties.entrySet().stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .map(entry -> new StarProperty<>(entry.getKey(), entry.getValue(), this))
                        .iterator();
            }
        }

        @Override
        public <U> Property<U> property(final String key, final U value) {
            Map<String, Object> properties = metaProperties.get(this.id);
            if (null == properties) {
                properties = new HashMap<>();
                metaProperties.put(this.id, properties);
            }
            properties.put(key, value);
            return new StarProperty<>(key, value, this);
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }
    }


    ///////////////////////////////
    //// STAR ADJACENT VERTEX ////
    //////////////////////////////

    public class StarAdjacentVertex implements Vertex {

        private final Object id;

        protected StarAdjacentVertex(final Object id) {
            this.id = id;
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            if (!starVertex.equals(inVertex))
                throw new IllegalStateException("An adjacent vertex can only connect to the star vertex: " + starVertex);
            return starVertex.addInEdge(label, this, keyValues);
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            throw Element.Exceptions.propertyAdditionNotSupported();
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
            throw Element.Exceptions.propertyAdditionNotSupported();
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            return Collections.emptyIterator();  // TODO: just return to starVertex?
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
            return Collections.emptyIterator();  // TODO: just return star vertex?
        }

        @Override
        public Object id() {
            return this.id;
        }

        @Override
        public String label() {
            return Vertex.DEFAULT_LABEL;
        }

        @Override
        public Graph graph() {
            return StarGraph.this;
        }

        @Override
        public void remove() {
            throw Vertex.Exceptions.vertexRemovalNotSupported();
        }

        @Override
        public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
            return Collections.emptyIterator();
        }

        @Override
        public boolean equals(final Object other) {
            return ElementHelper.areEqual(this, other);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode(this);
        }

        @Override
        public String toString() {
            return StringFactory.vertexString(this);
        }
    }

    ////////////////////
    //// STAR EDGE ////
    ///////////////////

    public abstract class StarEdge extends StarElement implements Edge {

        protected final Object otherId;

        public StarEdge(final Object id, final String label, final Object otherId) {
            super(id, label);
            this.otherId = otherId;
        }

        @Override
        public <V> Property<V> property(final String key, final V value) {
            Map<String, Object> properties = edgeProperties.get(this.id);
            if (null == properties) {
                properties = new HashMap<>();
                edgeProperties.put(this.id, properties);
            }
            properties.put(key, value);
            return new StarProperty<>(key, value, this);
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            Map<String, Object> properties = edgeProperties.get(this.id);
            if (null == properties || properties.isEmpty())
                return Collections.emptyIterator();
            else if (propertyKeys.length == 0)
                return (Iterator) properties.entrySet().stream()
                        .map(entry -> new StarProperty<>(entry.getKey(), entry.getValue(), this))
                        .iterator();
            else if (propertyKeys.length == 1) {
                final Object v = properties.get(propertyKeys[0]);
                return null == v ?
                        Collections.emptyIterator() :
                        (Iterator) IteratorUtils.of(new StarProperty<>(propertyKeys[0], v, this));
            } else {
                return (Iterator) properties.entrySet().stream()
                        .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                        .map(entry -> new StarProperty<>(entry.getKey(), entry.getValue(), this))
                        .iterator();
            }
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            if (direction.equals(Direction.OUT))
                return IteratorUtils.of(this.outVertex());
            else if (direction.equals(Direction.IN))
                return IteratorUtils.of(this.inVertex());
            else
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }

        @Override
        public void remove() {
            throw Edge.Exceptions.edgeRemovalNotSupported();
        }

        @Override
        public String toString() {
            return StringFactory.edgeString(this);
        }
    }

    public final class StarOutEdge extends StarEdge {

        public StarOutEdge(final Object id, final String label, final Object otherId) {
            super(id, label, otherId);
        }

        @Override
        public Vertex outVertex() {
            return starVertex;
        }

        @Override
        public Vertex inVertex() {
            return new StarAdjacentVertex(this.otherId);
        }
    }

    public final class StarInEdge extends StarEdge {

        public StarInEdge(final Object id, final String label, final Object otherId) {
            super(id, label, otherId);
        }

        @Override
        public Vertex outVertex() {
            return new StarAdjacentVertex(this.otherId);
        }

        @Override
        public Vertex inVertex() {
            return starVertex;
        }
    }

    ////////////////////////
    //// STAR PROPERTY ////
    ///////////////////////

    public final class StarProperty<V> implements Property<V> {

        private final String key;
        private final V value;
        private final Element element;

        public StarProperty(final String key, final V value, final Element element) {
            this.key = key.intern();
            this.value = value;
            this.element = element;
        }

        @Override
        public String key() {
            return this.key;
        }

        @Override
        public V value() throws NoSuchElementException {
            return this.value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public Element element() {
            return this.element;
        }

        @Override
        public void remove() {
            throw Exceptions.propertyRemovalNotSupported();
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }

        @Override
        public boolean equals(final Object object) {
            return ElementHelper.areEqual(this, object);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode(this);
        }
    }
}
