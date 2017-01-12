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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * An interface that provides methods for detached properties and elements to be re-attached to the {@link Graph}.
 * There are two general ways in which they can be attached: {@link Method#get} or {@link Method#create}.
 * A {@link Method#get} will find the property/element at the host location and return it.
 * A {@link Method#create} will create the property/element at the host location and return it.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Attachable<V> {

    /**
     * Get the raw object trying to be attached.
     *
     * @return the raw object to attach
     */
    public V get();

    /**
     * Provide a way to attach an {@link Attachable} implementation to a host.  Note that the context of the host
     * is not defined by way of the attachment method itself that is supplied as an argument.  It is up to the
     * implementer to supply that context.
     *
     * @param method a {@link Function} that takes an {@link Attachable} and returns the "re-attached" object
     * @return the return value of the {@code method}
     * @throws IllegalStateException if the {@link Attachable} is not a "graph" object (i.e. host or
     *                               attachable don't work together)
     */
    public default V attach(final Function<Attachable<V>, V> method) throws IllegalStateException {
        return method.apply(this);
    }

    /**
     * A collection of general methods of attachment. Note that more efficient methods of attachment might exist
     * if the user knows the source data being attached and the features of the graph that the data is being
     * attached to.
     */
    public static class Method {
        public static <V> Function<Attachable<V>, V> get(final Host hostVertexOrGraph) {
            return (Attachable<V> attachable) -> {
                final Object base = attachable.get();
                if (base instanceof Vertex) {
                    final Optional<Vertex> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getVertex((Attachable<Vertex>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertex((Attachable<Vertex>) attachable, (Vertex) hostVertexOrGraph);
                    return (V) optional.orElseThrow(() -> hostVertexOrGraph instanceof Graph ?
                            Attachable.Exceptions.canNotGetAttachableFromHostGraph(attachable, (Graph) hostVertexOrGraph) :
                            Attachable.Exceptions.canNotGetAttachableFromHostVertex(attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof Edge) {
                    final Optional<Edge> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getEdge((Attachable<Edge>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getEdge((Attachable<Edge>) attachable, (Vertex) hostVertexOrGraph);
                    return (V) optional.orElseThrow(() -> hostVertexOrGraph instanceof Graph ?
                            Attachable.Exceptions.canNotGetAttachableFromHostGraph(attachable, (Graph) hostVertexOrGraph) :
                            Attachable.Exceptions.canNotGetAttachableFromHostVertex(attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof VertexProperty) {
                    final Optional<VertexProperty> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getVertexProperty((Attachable<VertexProperty>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertexProperty((Attachable<VertexProperty>) attachable, (Vertex) hostVertexOrGraph);
                    return (V) optional.orElseThrow(() -> hostVertexOrGraph instanceof Graph ?
                            Attachable.Exceptions.canNotGetAttachableFromHostGraph(attachable, (Graph) hostVertexOrGraph) :
                            Attachable.Exceptions.canNotGetAttachableFromHostVertex(attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof Property) {
                    final Optional<Property> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getProperty((Attachable<Property>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getProperty((Attachable<Property>) attachable, (Vertex) hostVertexOrGraph);
                    return (V) optional.orElseThrow(() -> hostVertexOrGraph instanceof Graph ?
                            Attachable.Exceptions.canNotGetAttachableFromHostGraph(attachable, (Graph) hostVertexOrGraph) :
                            Attachable.Exceptions.canNotGetAttachableFromHostVertex(attachable, (Vertex) hostVertexOrGraph));
                } else
                    throw Attachable.Exceptions.providedAttachableMustContainAGraphObject(attachable);
            };
        }

        public static <V> Function<Attachable<V>, V> getOrCreate(final Host hostVertexOrGraph) {
            return (Attachable<V> attachable) -> {
                final Object base = attachable.get();
                if (base instanceof Vertex) {
                    return (V) (hostVertexOrGraph instanceof Graph ?
                            Method.getVertex((Attachable<Vertex>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertex((Attachable<Vertex>) attachable, (Vertex) hostVertexOrGraph))
                            .orElseGet(() -> hostVertexOrGraph instanceof Graph ?
                                    Method.createVertex((Attachable<Vertex>) attachable, (Graph) hostVertexOrGraph) :
                                    Method.createVertex((Attachable<Vertex>) attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof Edge) {
                    return (V) (hostVertexOrGraph instanceof Graph ?
                            Method.getEdge((Attachable<Edge>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getEdge((Attachable<Edge>) attachable, (Vertex) hostVertexOrGraph))
                            .orElseGet(() -> hostVertexOrGraph instanceof Graph ?
                                    Method.createEdge((Attachable<Edge>) attachable, (Graph) hostVertexOrGraph) :
                                    Method.createEdge((Attachable<Edge>) attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof VertexProperty) {
                    return (V) (hostVertexOrGraph instanceof Graph ?
                            Method.getVertexProperty((Attachable<VertexProperty>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertexProperty((Attachable<VertexProperty>) attachable, (Vertex) hostVertexOrGraph))
                            .orElseGet(() -> hostVertexOrGraph instanceof Graph ?
                                    Method.createVertexProperty((Attachable<VertexProperty>) attachable, (Graph) hostVertexOrGraph) :
                                    Method.createVertexProperty((Attachable<VertexProperty>) attachable, (Vertex) hostVertexOrGraph));
                } else if (base instanceof Property) {
                    return (V) (hostVertexOrGraph instanceof Graph ?
                            Method.getProperty((Attachable<Property>) attachable, (Graph) hostVertexOrGraph) :
                            Method.getProperty((Attachable<Property>) attachable, (Vertex) hostVertexOrGraph))
                            .orElseGet(() -> hostVertexOrGraph instanceof Graph ?
                                    Method.createProperty((Attachable<Property>) attachable, (Graph) hostVertexOrGraph) :
                                    Method.createProperty((Attachable<Property>) attachable, (Vertex) hostVertexOrGraph));
                } else
                    throw Attachable.Exceptions.providedAttachableMustContainAGraphObject(attachable);
            };
        }

        public static <V> Function<Attachable<V>, V> create(final Host hostVertexOrGraph) {
            return (Attachable<V> attachable) -> {
                final Object base = attachable.get();
                if (base instanceof Vertex) {
                    return hostVertexOrGraph instanceof Graph ?
                            (V) Method.createVertex((Attachable<Vertex>) attachable, (Graph) hostVertexOrGraph) :
                            (V) Method.createVertex((Attachable<Vertex>) attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof Edge) {
                    return hostVertexOrGraph instanceof Graph ?
                            (V) Method.createEdge((Attachable<Edge>) attachable, (Graph) hostVertexOrGraph) :
                            (V) Method.createEdge((Attachable<Edge>) attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof VertexProperty) {
                    return hostVertexOrGraph instanceof Graph ?
                            (V) Method.createVertexProperty((Attachable<VertexProperty>) attachable, (Graph) hostVertexOrGraph) :
                            (V) Method.createVertexProperty((Attachable<VertexProperty>) attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof Property) {
                    return hostVertexOrGraph instanceof Graph ?
                            (V) Method.createProperty((Attachable<Property>) attachable, (Graph) hostVertexOrGraph) :
                            (V) Method.createProperty((Attachable<Property>) attachable, (Vertex) hostVertexOrGraph);
                } else
                    throw Attachable.Exceptions.providedAttachableMustContainAGraphObject(attachable);
            };
        }

        ///////////////////

        ///// GET HELPER METHODS

        public static Optional<Vertex> getVertex(final Attachable<Vertex> attachableVertex, final Graph hostGraph) {
            final Iterator<Vertex> vertexIterator = hostGraph.vertices(attachableVertex.get().id());
            return vertexIterator.hasNext() ? Optional.of(vertexIterator.next()) : Optional.empty();
        }

        public static Optional<Vertex> getVertex(final Attachable<Vertex> attachableVertex, final Vertex hostVertex) {
            return ElementHelper.areEqual(attachableVertex.get(), hostVertex) ? Optional.of(hostVertex) : Optional.empty();
        }

        public static Optional<Edge> getEdge(final Attachable<Edge> attachableEdge, final Graph hostGraph) {
            final Iterator<Edge> edgeIterator = hostGraph.edges(attachableEdge.get().id());
            return edgeIterator.hasNext() ? Optional.of(edgeIterator.next()) : Optional.empty();
        }

        public static Optional<Edge> getEdge(final Attachable<Edge> attachableEdge, final Vertex hostVertex) {
            final Edge baseEdge = attachableEdge.get();
            final Iterator<Edge> edgeIterator = hostVertex.edges(Direction.OUT, attachableEdge.get().label());
            while (edgeIterator.hasNext()) {
                final Edge edge = edgeIterator.next();
                if (ElementHelper.areEqual(edge, baseEdge))
                    return Optional.of(edge);
            }
            return Optional.empty();
        }

        public static Optional<VertexProperty> getVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Graph hostGraph) {
            final VertexProperty baseVertexProperty = attachableVertexProperty.get();
            final Iterator<Vertex> vertexIterator = hostGraph.vertices(baseVertexProperty.element().id());
            if (vertexIterator.hasNext()) {
                final Iterator<VertexProperty<Object>> vertexPropertyIterator = vertexIterator.next().properties(baseVertexProperty.key());
                while (vertexPropertyIterator.hasNext()) {
                    final VertexProperty vertexProperty = vertexPropertyIterator.next();
                    if (ElementHelper.areEqual(vertexProperty, baseVertexProperty))
                        return Optional.of(vertexProperty);
                }
            }
            return Optional.empty();
        }

        public static Optional<VertexProperty> getVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Vertex hostVertex) {
            final VertexProperty baseVertexProperty = attachableVertexProperty.get();
            final Iterator<VertexProperty<Object>> vertexPropertyIterator = hostVertex.properties(baseVertexProperty.key());
            while (vertexPropertyIterator.hasNext()) {
                final VertexProperty vertexProperty = vertexPropertyIterator.next();
                if (ElementHelper.areEqual(vertexProperty, baseVertexProperty))
                    return Optional.of(vertexProperty);
            }
            return Optional.empty();
        }

        public static Optional<Property> getProperty(final Attachable<Property> attachableProperty, final Graph hostGraph) {
            final Property baseProperty = attachableProperty.get();
            final Element propertyElement = attachableProperty.get().element();
            if (propertyElement instanceof Vertex) {
                return (Optional) Method.getVertexProperty((Attachable) attachableProperty, hostGraph);
            } else if (propertyElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostGraph.edges(propertyElement.id());
                while (edgeIterator.hasNext()) {
                    final Property property = edgeIterator.next().property(baseProperty.key());
                    if (property.isPresent() && property.value().equals(baseProperty.value()))
                        return Optional.of(property);
                }
                return Optional.empty();
            } else { // vertex property
                final Iterator<Vertex> vertexIterator = hostGraph.vertices(((VertexProperty) propertyElement).element().id());
                if (vertexIterator.hasNext()) {
                    final Iterator<VertexProperty<Object>> vertexPropertyIterator = vertexIterator.next().properties();
                    while (vertexPropertyIterator.hasNext()) {
                        final VertexProperty vertexProperty = vertexPropertyIterator.next();
                        if (ElementHelper.areEqual(vertexProperty, baseProperty.element())) {
                            final Property property = vertexProperty.property(baseProperty.key());
                            if (property.isPresent() && property.value().equals(baseProperty.value()))
                                return Optional.of(property);
                            else
                                return Optional.empty();
                        }
                    }
                }
                return Optional.empty();
            }
        }

        public static Optional<Property> getProperty(final Attachable<Property> attachableProperty, final Vertex hostVertex) {
            final Property baseProperty = attachableProperty.get();
            final Element propertyElement = attachableProperty.get().element();
            if (propertyElement instanceof Vertex) {
                return (Optional) Method.getVertexProperty((Attachable) attachableProperty, hostVertex);
            } else if (propertyElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostVertex.edges(Direction.OUT);
                while (edgeIterator.hasNext()) {
                    final Edge edge = edgeIterator.next();
                    if (ElementHelper.areEqual(edge, propertyElement)) {
                        final Property property = edge.property(baseProperty.key());
                        if (ElementHelper.areEqual(baseProperty, property))
                            return Optional.of(property);
                    }
                }
                return Optional.empty();
            } else { // vertex property
                final Iterator<VertexProperty<Object>> vertexPropertyIterator = hostVertex.properties();
                while (vertexPropertyIterator.hasNext()) {
                    final VertexProperty vertexProperty = vertexPropertyIterator.next();
                    if (ElementHelper.areEqual(vertexProperty, baseProperty.element())) {
                        final Property property = vertexProperty.property(baseProperty.key());
                        if (property.isPresent() && property.value().equals(baseProperty.value()))
                            return Optional.of(property);
                        else
                            return Optional.empty();
                    }
                }
                return Optional.empty();
            }
        }

        ///// CREATE HELPER METHODS

        public static Vertex createVertex(final Attachable<Vertex> attachableVertex, final Graph hostGraph) {
            final Vertex baseVertex = attachableVertex.get();
            final Vertex vertex = hostGraph.features().vertex().willAllowId(baseVertex.id()) ?
                    hostGraph.addVertex(T.id, baseVertex.id(), T.label, baseVertex.label()) :
                    hostGraph.addVertex(T.label, baseVertex.label());
            baseVertex.properties().forEachRemaining(vp -> {
                final VertexProperty vertexProperty = hostGraph.features().vertex().properties().willAllowId(vp.id()) ?
                        vertex.property(hostGraph.features().vertex().getCardinality(vp.key()), vp.key(), vp.value(), T.id, vp.id()) :
                        vertex.property(hostGraph.features().vertex().getCardinality(vp.key()), vp.key(), vp.value());
                vp.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
            });
            return vertex;
        }

        public static Vertex createVertex(final Attachable<Vertex> attachableVertex, final Vertex hostVertex) {
            throw new IllegalStateException("It is not possible to create a vertex at a host vertex");
        }

        public static Edge createEdge(final Attachable<Edge> attachableEdge, final Graph hostGraph) {
            final Edge baseEdge = attachableEdge.get();
            Iterator<Vertex> vertices = hostGraph.vertices(baseEdge.outVertex().id());
            final Vertex outV = vertices.hasNext() ? vertices.next() : hostGraph.features().vertex().willAllowId(baseEdge.outVertex().id()) ? hostGraph.addVertex(T.id, baseEdge.outVertex().id()) : hostGraph.addVertex();
            vertices = hostGraph.vertices(baseEdge.inVertex().id());
            final Vertex inV = vertices.hasNext() ? vertices.next() : hostGraph.features().vertex().willAllowId(baseEdge.inVertex().id()) ? hostGraph.addVertex(T.id, baseEdge.inVertex().id()) : hostGraph.addVertex();
            if (ElementHelper.areEqual(outV, inV)) {
                final Iterator<Edge> itty = outV.edges(Direction.OUT, baseEdge.label());
                while (itty.hasNext()) {
                    final Edge e = itty.next();
                    if (ElementHelper.areEqual(baseEdge, e))
                        return e;
                }
            }
            final Edge e = hostGraph.features().edge().willAllowId(baseEdge.id()) ? outV.addEdge(baseEdge.label(), inV, T.id, baseEdge.id()) : outV.addEdge(baseEdge.label(), inV);
            baseEdge.properties().forEachRemaining(p -> e.property(p.key(), p.value()));
            return e;
        }

        public static Edge createEdge(final Attachable<Edge> attachableEdge, final Vertex hostVertex) {
            return Method.createEdge(attachableEdge, hostVertex.graph()); // TODO (make local to vertex)
        }

        public static VertexProperty createVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Graph hostGraph) {
            final VertexProperty<Object> baseVertexProperty = attachableVertexProperty.get();
            final Iterator<Vertex> vertexIterator = hostGraph.vertices(baseVertexProperty.element().id());
            if (vertexIterator.hasNext()) {
                final VertexProperty vertexProperty = hostGraph.features().vertex().properties().willAllowId(baseVertexProperty.id()) ?
                        vertexIterator.next().property(hostGraph.features().vertex().getCardinality(baseVertexProperty.key()), baseVertexProperty.key(), baseVertexProperty.value(), T.id, baseVertexProperty.id()) :
                        vertexIterator.next().property(hostGraph.features().vertex().getCardinality(baseVertexProperty.key()), baseVertexProperty.key(), baseVertexProperty.value());
                baseVertexProperty.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
                return vertexProperty;
            }
            throw new IllegalStateException("Could not find vertex to create the attachable vertex property on");
        }

        public static VertexProperty createVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Vertex hostVertex) {
            final VertexProperty<Object> baseVertexProperty = attachableVertexProperty.get();
            final VertexProperty vertexProperty = hostVertex.graph().features().vertex().properties().willAllowId(baseVertexProperty.id()) ?
                    hostVertex.property(hostVertex.graph().features().vertex().getCardinality(baseVertexProperty.key()), baseVertexProperty.key(), baseVertexProperty.value(), T.id, baseVertexProperty.id()) :
                    hostVertex.property(hostVertex.graph().features().vertex().getCardinality(baseVertexProperty.key()), baseVertexProperty.key(), baseVertexProperty.value());
            baseVertexProperty.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
            return vertexProperty;
        }

        public static Property createProperty(final Attachable<Property> attachableProperty, final Graph hostGraph) {
            final Property baseProperty = attachableProperty.get();
            final Element baseElement = baseProperty.element();
            if (baseElement instanceof Vertex) {
                return Method.createVertexProperty((Attachable) attachableProperty, hostGraph);
            } else if (baseElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostGraph.edges(baseElement.id());
                if (edgeIterator.hasNext())
                    return edgeIterator.next().property(baseProperty.key(), baseProperty.value());
                throw new IllegalStateException("Could not find edge to create the attachable property on");
            } else { // vertex property
                final Iterator<Vertex> vertexIterator = hostGraph.vertices(((VertexProperty) baseElement).element().id());
                if (vertexIterator.hasNext()) {
                    final Vertex vertex = vertexIterator.next();
                    final Iterator<VertexProperty<Object>> vertexPropertyIterator = vertex.properties(((VertexProperty) baseElement).key());
                    while (vertexPropertyIterator.hasNext()) {
                        final VertexProperty<Object> vp = vertexPropertyIterator.next();
                        if (ElementHelper.areEqual(vp, baseElement))
                            return vp.property(baseProperty.key(), baseProperty.value());
                    }
                }
                throw new IllegalStateException("Could not find vertex property to create the attachable property on");
            }
        }

        public static Property createProperty(final Attachable<Property> attachableProperty, final Vertex hostVertex) {
            final Property baseProperty = attachableProperty.get();
            final Element baseElement = baseProperty.element();
            if (baseElement instanceof Vertex) {
                return Method.createVertexProperty((Attachable) attachableProperty, hostVertex);
            } else if (baseElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostVertex.edges(Direction.OUT);
                if (edgeIterator.hasNext())
                    return edgeIterator.next().property(baseProperty.key(), baseProperty.value());
                throw new IllegalStateException("Could not find edge to create the property on");
            } else { // vertex property
                final Iterator<VertexProperty<Object>> vertexPropertyIterator = hostVertex.properties(((VertexProperty) baseElement).key());
                while (vertexPropertyIterator.hasNext()) {
                    final VertexProperty<Object> vp = vertexPropertyIterator.next();
                    if (ElementHelper.areEqual(vp, baseElement))
                        return vp.property(baseProperty.key(), baseProperty.value());
                }
                throw new IllegalStateException("Could not find vertex property to create the attachable property on");
            }
        }
    }

    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalStateException canNotGetAttachableFromHostVertex(final Attachable<?> attachable, final Vertex hostVertex) {
            return new IllegalStateException("Can not get the attachable from the host vertex: " + attachable + "-/->" + hostVertex);
        }

        public static IllegalStateException canNotGetAttachableFromHostGraph(final Attachable<?> attachable, final Graph hostGraph) {
            return new IllegalStateException("Can not get the attachable from the host vertex: " + attachable + "-/->" + hostGraph);
        }

        public static IllegalArgumentException providedAttachableMustContainAGraphObject(final Attachable<?> attachable) {
            return new IllegalArgumentException("The provided attachable must contain a graph object: " + attachable);
        }
    }

}