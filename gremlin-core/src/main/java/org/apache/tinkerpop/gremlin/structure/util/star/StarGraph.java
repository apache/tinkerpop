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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * A {@code StarGraph} is a form of {@link Attachable} (though the {@link Graph} implementation does not implement
 * that interface itself).  It is a very limited {@link Graph} implementation that holds a single {@link Vertex}
 * and its related properties and edges (and their properties).  It is designed to be an efficient memory
 * representation of this data structure, thus making it good for network and disk-based serialization.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StarGraph implements Graph, Serializable {

    private static final Configuration STAR_GRAPH_CONFIGURATION = new BaseConfiguration();

    static {
        STAR_GRAPH_CONFIGURATION.setProperty(Graph.GRAPH, StarGraph.class.getCanonicalName());
    }

    protected Long nextId = 0l;
    protected StarVertex starVertex = null;
    protected Map<Object, Map<String, Object>> edgeProperties = null;
    protected Map<Object, Map<String, Object>> metaProperties = null;

    private StarGraph() {
    }

    /**
     * Gets the {@link Vertex} representative of the {@link StarGraph}.
     */
    public StarVertex getStarVertex() {
        return this.starVertex;
    }

    private Long nextId() {
        return this.nextId++;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        if (null == this.starVertex) {
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            this.starVertex = new StarVertex(ElementHelper.getIdValue(keyValues).orElse(this.nextId()), ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL));
            ElementHelper.attachProperties(this.starVertex, VertexProperty.Cardinality.list, keyValues); // TODO: is this smart? I say no... cause vertex property ids are not preserved.
            return this.starVertex;
        } else
            return new StarAdjacentVertex(ElementHelper.getIdValue(keyValues).orElse(this.nextId()));
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
                        null == this.starVertex.inEdges ? Stream.empty() : this.starVertex.inEdges.values().stream(),
                        null == this.starVertex.outEdges ? Stream.empty() : this.starVertex.outEdges.values().stream())
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
    public Features features() {
        return StarGraphFeatures.INSTANCE;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "starOf:" + this.starVertex);
    }

    /**
     * Creates an empty {@link StarGraph}.
     */
    public static StarGraph open() {
        return new StarGraph();
    }

    /**
     * Creates a new {@link StarGraph} from a {@link Vertex}.
     */
    public static StarGraph of(final Vertex vertex) {
        if (vertex instanceof StarVertex) return (StarGraph) vertex.graph();
        // else convert to a star graph
        final StarGraph starGraph = new StarGraph();
        final StarVertex starVertex = (StarVertex) starGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());

        final boolean supportsMetaProperties = vertex.graph().features().vertex().supportsMetaProperties();

        vertex.properties().forEachRemaining(vp -> {
            final VertexProperty<?> starVertexProperty = starVertex.property(VertexProperty.Cardinality.list, vp.key(), vp.value(), T.id, vp.id());
            if (supportsMetaProperties) vp.properties().forEachRemaining(p -> starVertexProperty.property(p.key(), p.value()));
        });
        vertex.edges(Direction.IN).forEachRemaining(edge -> {
            final Edge starEdge = starVertex.addInEdge(edge.label(), starGraph.addVertex(T.id, edge.outVertex().id()), T.id, edge.id());
            edge.properties().forEachRemaining(p -> starEdge.property(p.key(), p.value()));
        });

        vertex.edges(Direction.OUT).forEachRemaining(edge -> {
            if (!ElementHelper.areEqual(starVertex, edge.inVertex())) { // only do a self loop once
                final Edge starEdge = starVertex.addOutEdge(edge.label(), starGraph.addVertex(T.id, edge.inVertex().id()), T.id, edge.id());
                edge.properties().forEachRemaining(p -> starEdge.property(p.key(), p.value()));
            }
        });
        return starGraph;
    }

    ///////////////////////
    //// STAR ELEMENT ////
    //////////////////////

    public abstract class StarElement<E extends Element> implements Element, Attachable<E> {

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

        @Override
        public E get() {
            return (E) this;
        }
    }

    //////////////////////
    //// STAR VERTEX ////
    /////////////////////

    public final class StarVertex extends StarElement<Vertex> implements Vertex {

        protected Map<String, List<Edge>> outEdges = null;
        protected Map<String, List<Edge>> inEdges = null;
        protected Map<String, List<VertexProperty>> vertexProperties = null;

        public StarVertex(final Object id, final String label) {
            super(id, label);
        }

        public void dropEdges() {
            this.outEdges = null;
            this.inEdges = null;
        }

        public void dropVertexProperties(final String... propertyKeys) {
            if (null != this.vertexProperties) {
                for (final String key : propertyKeys) {
                    this.vertexProperties.remove(key);
                }
            }
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            return this.addOutEdge(label, inVertex, keyValues);
        }

        @Override
        public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
            ElementHelper.validateProperty(key, value);
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            return this.property(VertexProperty.Cardinality.single, key, value, keyValues);
        }

        public Edge addOutEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            ElementHelper.validateLabel(label);
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            if (null == this.outEdges)
                this.outEdges = new HashMap<>();
            List<Edge> outE = this.outEdges.get(label);
            if (null == outE) {
                outE = new ArrayList<>();
                this.outEdges.put(label, outE);
            }
            final StarEdge outEdge = new StarOutEdge(ElementHelper.getIdValue(keyValues).orElse(nextId()), label, inVertex.id());
            ElementHelper.attachProperties(outEdge, keyValues);
            outE.add(outEdge);
            return outEdge;
        }

        public Edge addInEdge(final String label, final Vertex outVertex, final Object... keyValues) {
            ElementHelper.validateLabel(label);
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            if (null == this.inEdges)
                this.inEdges = new HashMap<>();
            List<Edge> inE = this.inEdges.get(label);
            if (null == inE) {
                inE = new ArrayList<>();
                this.inEdges.put(label, inE);
            }
            final StarEdge inEdge = new StarInEdge(ElementHelper.getIdValue(keyValues).orElse(nextId()), label, outVertex.id());
            ElementHelper.attachProperties(inEdge, keyValues);
            inE.add(inEdge);
            return inEdge;
        }

        @Override
        public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, V value, final Object... keyValues) {
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            if (null == this.vertexProperties)
                this.vertexProperties = new HashMap<>();
            final List<VertexProperty> list = cardinality.equals(VertexProperty.Cardinality.single) ? new ArrayList<>(1) : this.vertexProperties.getOrDefault(key, new ArrayList<>());
            final VertexProperty<V> vertexProperty = new StarVertexProperty<>(ElementHelper.getIdValue(keyValues).orElse(nextId()), key, value);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            list.add(vertexProperty);
            this.vertexProperties.put(key, list);
            return vertexProperty;
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            if (direction.equals(Direction.OUT)) {
                return null == this.outEdges ? Collections.emptyIterator() : edgeLabels.length == 0 ?
                        IteratorUtils.flatMap(this.outEdges.values().iterator(), List::iterator) :
                        this.outEdges.entrySet().stream()
                                .filter(entry -> ElementHelper.keyExists(entry.getKey(), edgeLabels))
                                .map(Map.Entry::getValue)
                                .flatMap(List::stream)
                                .iterator();
            } else if (direction.equals(Direction.IN)) {
                return null == this.inEdges ? Collections.emptyIterator() : edgeLabels.length == 0 ?
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
            if (null == this.vertexProperties || this.vertexProperties.isEmpty())
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

    public final class StarVertexProperty<V> extends StarElement<VertexProperty<V>> implements VertexProperty<V> {

        private final V value;

        private StarVertexProperty(final Object id, final String key, final V value) {
            super(id, key);
            this.value = value;
        }

        @Override
        public String key() {
            return this.label();
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
            return StarGraph.this.starVertex;
        }

        @Override
        public void remove() {
            if (null != StarGraph.this.starVertex.vertexProperties)
                StarGraph.this.starVertex.vertexProperties.get(this.label()).remove(this);
        }

        @Override
        public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
            final Map<String, Object> properties = null == metaProperties ? null : metaProperties.get(this.id);
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
            if (null == metaProperties)
                metaProperties = new HashMap<>();
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

        private StarAdjacentVertex(final Object id) {
            this.id = id;
        }

        @Override
        public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
            if (inVertex.equals(starVertex))
                return starVertex.addInEdge(label, this, keyValues);
            else
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
        public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
            throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
            throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
        }

        @Override
        public Object id() {
            return this.id;
        }

        @Override
        public String label() {
            throw GraphComputer.Exceptions.adjacentVertexLabelsCanNotBeRead();
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
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
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

    public abstract class StarEdge extends StarElement<Edge> implements Edge {

        protected final Object otherId;

        private StarEdge(final Object id, final String label, final Object otherId) {
            super(id, label);
            this.otherId = otherId;
        }

        @Override
        public <V> Property<V> property(final String key, final V value) {
            ElementHelper.validateProperty(key, value);
            if (null == edgeProperties)
                edgeProperties = new HashMap<>();
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
            Map<String, Object> properties = null == edgeProperties ? null : edgeProperties.get(this.id);
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

        private StarOutEdge(final Object id, final String label, final Object otherId) {
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

        private StarInEdge(final Object id, final String label, final Object otherId) {
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

    public final class StarProperty<V> implements Property<V>, Attachable<Property<V>> {

        private final String key;
        private final V value;
        private final Element element;

        private StarProperty(final String key, final V value, final Element element) {
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
            throw Property.Exceptions.propertyRemovalNotSupported();
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

        @Override
        public Property<V> get() {
            return this;
        }
    }

    public static class StarGraphFeatures implements Features {
        public static final StarGraphFeatures INSTANCE = new StarGraphFeatures();

        private StarGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return StarGraphGraphFeatures.INSTANCE;
        }

        @Override
        public EdgeFeatures edge() {
            return StarGraphEdgeFeatures.INSTANCE;
        }

        @Override
        public VertexFeatures vertex() {
            return StarGraphVertexFeatures.INSTANCE;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    static class StarGraphVertexFeatures implements Features.VertexFeatures {
        public static final StarGraphVertexFeatures INSTANCE = new StarGraphVertexFeatures();

        private StarGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return StarGraphVertexPropertyFeatures.INSTANCE;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

    static class StarGraphEdgeFeatures implements Features.EdgeFeatures {
        public static final StarGraphEdgeFeatures INSTANCE = new StarGraphEdgeFeatures();

        private StarGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

    static class StarGraphGraphFeatures implements Features.GraphFeatures {
        public static final StarGraphGraphFeatures INSTANCE = new StarGraphGraphFeatures();

        private StarGraphGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    static class StarGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {
        public static final StarGraphVertexPropertyFeatures INSTANCE = new StarGraphVertexPropertyFeatures();

        private StarGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

}
