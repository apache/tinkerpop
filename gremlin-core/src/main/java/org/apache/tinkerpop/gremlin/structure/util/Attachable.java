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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * An interface that provides methods for detached properties and elements to be re-attached to the {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Attachable<T> {
    public enum Method implements BiFunction<Attachable, Object, Object> {

        GET {
            @Override
            public Object apply(final Attachable attachable, final Object hostVertexOrGraph) {
                final Object base = attachable.get();
                if (base instanceof Vertex) {
                    final Optional<Vertex> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getVertex(attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertex(attachable, (Vertex) hostVertexOrGraph);
                    return optional.orElseThrow(() -> new IllegalStateException("Can not get the following attachable from " + hostVertexOrGraph + ": " + attachable));
                } else if (base instanceof Edge) {
                    final Optional<Edge> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getEdge(attachable, (Graph) hostVertexOrGraph) :
                            Method.getEdge(attachable, (Vertex) hostVertexOrGraph);
                    return optional.orElseThrow(() -> new IllegalStateException("Can not get the following attachable from " + hostVertexOrGraph + ": " + attachable));
                } else if (base instanceof VertexProperty) {
                    final Optional<VertexProperty> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getVertexProperty(attachable, (Graph) hostVertexOrGraph) :
                            Method.getVertexProperty(attachable, (Vertex) hostVertexOrGraph);
                    return optional.orElseThrow(() -> new IllegalStateException("Can not get the following attachable from " + hostVertexOrGraph + ": " + attachable));
                } else if (base instanceof Property) {
                    final Optional<Property> optional = hostVertexOrGraph instanceof Graph ?
                            Method.getProperty(attachable, (Graph) hostVertexOrGraph) :
                            Method.getProperty(attachable, (Vertex) hostVertexOrGraph);
                    return optional.orElseThrow(() -> new IllegalStateException("Can not get the following attachable from " + hostVertexOrGraph + ": " + attachable));
                } else {
                    throw new IllegalArgumentException("The attachable must contain an graph object");
                }
            }
        },

        CREATE {
            @Override
            public Object apply(final Attachable attachable, final Object hostVertexOrGraph) {
                final Object base = attachable.get();
                if (base instanceof Vertex) {
                    return hostVertexOrGraph instanceof Graph ?
                            Method.createVertex(attachable, (Graph) hostVertexOrGraph) :
                            Method.createVertex(attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof Edge) {
                    return hostVertexOrGraph instanceof Graph ?
                            Method.createEdge(attachable, (Graph) hostVertexOrGraph) :
                            Method.createEdge(attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof VertexProperty) {
                    return hostVertexOrGraph instanceof Graph ?
                            Method.createVertexProperty(attachable, (Graph) hostVertexOrGraph) :
                            Method.createVertexProperty(attachable, (Vertex) hostVertexOrGraph);
                } else if (base instanceof Property) {
                    return hostVertexOrGraph instanceof Graph ?
                            Method.createProperty(attachable, (Graph) hostVertexOrGraph) :
                            Method.createProperty(attachable, (Vertex) hostVertexOrGraph);
                } else {
                    throw new IllegalArgumentException("The attachable must contain an graph object");
                }
            }
        };

        ///////////////////

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
            final Object baseId = attachableEdge.get().id();
            final Iterator<Edge> edgeIterator = hostVertex.edges(Direction.OUT);
            while (edgeIterator.hasNext()) {
                final Edge edge = edgeIterator.next();
                if (edge.id().equals(baseId))
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
            if (propertyElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostGraph.edges(propertyElement.id());
                if (edgeIterator.hasNext()) {
                    final Property property = edgeIterator.next().property(baseProperty.key());
                    if (property.isPresent() && property.value().equals(baseProperty.value()))
                        return Optional.of(property);
                }
                return Optional.empty();
            } else {
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
            if (propertyElement instanceof Edge) {
                final Iterator<Edge> edgeIterator = hostVertex.edges(Direction.OUT);
                if (edgeIterator.hasNext()) {
                    final Property property = edgeIterator.next().property(baseProperty.key());
                    if (property.isPresent() && property.value().equals(baseProperty.value()))
                        return Optional.of(property);
                }
                return Optional.empty();
            } else {
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

        /////

        public static Vertex createVertex(final Attachable<Vertex> attachableVertex, final Graph hostGraph) {
            final Vertex baseVertex = attachableVertex.get();
            final List<Object> keyValues = new ArrayList<>();
            keyValues.add(org.apache.tinkerpop.gremlin.process.traversal.T.id);
            keyValues.add(baseVertex.id());
            keyValues.add(org.apache.tinkerpop.gremlin.process.traversal.T.label);
            keyValues.add(baseVertex.label());
            final Vertex vertex = hostGraph.addVertex(keyValues.toArray());
            baseVertex.properties().forEachRemaining(vp -> {
                final VertexProperty vertexProperty = vertex.property(VertexProperty.Cardinality.list, vp.key(), vp.value());
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
            final Vertex outV = vertices.hasNext() ? vertices.next() : hostGraph.addVertex(org.apache.tinkerpop.gremlin.process.traversal.T.id, baseEdge.outVertex().id());
            vertices = hostGraph.vertices(baseEdge.inVertex().id());
            final Vertex inV = vertices.hasNext() ? vertices.next() : hostGraph.addVertex(org.apache.tinkerpop.gremlin.process.traversal.T.id, baseEdge.inVertex().id());
            if (ElementHelper.areEqual(outV, inV)) {
                final Iterator<Edge> itty = outV.edges(Direction.OUT, baseEdge.label());
                while (itty.hasNext()) {
                    final Edge e = itty.next();
                    if (ElementHelper.areEqual(baseEdge, e))
                        return e;
                }
            }
            final Edge e = outV.addEdge(baseEdge.label(), inV, org.apache.tinkerpop.gremlin.process.traversal.T.id, baseEdge.id());
            baseEdge.properties().forEachRemaining(p -> e.property(p.key(), p.value()));
            return e;
        }

        public static Edge createEdge(final Attachable<Edge> attachableEdge, final Vertex hostVertex) {
            return Method.createEdge(attachableEdge, hostVertex.graph());
        }

        public static VertexProperty createVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Graph hostGraph) {
            final VertexProperty<Object> baseVertexProperty = attachableVertexProperty.get();
            final Iterator<Vertex> vertexIterator = hostGraph.vertices(baseVertexProperty.element().id());
            if (vertexIterator.hasNext()) {
                final VertexProperty vertexProperty = vertexIterator.next().property(VertexProperty.Cardinality.list, baseVertexProperty.key(), baseVertexProperty.value(), org.apache.tinkerpop.gremlin.process.traversal.T.id, baseVertexProperty.id());
                baseVertexProperty.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
                return vertexProperty;
            }
            throw new IllegalStateException("Could not find vertex to add the vertex property to");
        }

        public static VertexProperty createVertexProperty(final Attachable<VertexProperty> attachableVertexProperty, final Vertex hostVertex) {
            final VertexProperty<Object> baseVertexProperty = attachableVertexProperty.get();
            final VertexProperty vertexProperty = hostVertex.property(VertexProperty.Cardinality.list, baseVertexProperty.key(), baseVertexProperty.value(), org.apache.tinkerpop.gremlin.process.traversal.T.id, baseVertexProperty.id());
            baseVertexProperty.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
            return vertexProperty;
        }

        public static Property createProperty(final Attachable<Property> attachableProperty, final Graph hostGraph) {
            return null;

        }

        public static Property createProperty(final Attachable<Property> attachableProperty, final Vertex hostVertex) {
            return null;
        }

    }

    public T get();

    public T attach(final Vertex hostVertex, final Method method) throws IllegalStateException;

    public T attach(final Graph hostGraph, final Method method) throws IllegalStateException;


    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalStateException canNotAttachVertexToHostVertex(final Attachable<Vertex> vertex, final Vertex hostVertex) {
            return new IllegalStateException("The provided vertex is not the host vertex: " + vertex + " does not equal " + hostVertex);
        }

        public static IllegalStateException canNotAttachVertexToHostGraph(final Attachable<Vertex> vertex, final Graph hostGraph) {
            return new IllegalStateException("The provided vertex could not be found in the host graph: " + vertex + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachEdgeToHostVertex(final Attachable<Edge> edge, final Vertex hostVertex) {
            return new IllegalStateException("The provided edge is not incident to the host vertex: " + edge + " not incident to " + hostVertex);
        }

        public static IllegalStateException canNotAttachEdgeToHostGraph(final Attachable<Edge> edge, final Graph hostGraph) {
            return new IllegalStateException("The provided edge could not be found in the host graph: " + edge + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachVertexPropertyToHostVertex(final Attachable<VertexProperty> vertexProperty, final Vertex hostVertex) {
            return new IllegalStateException("The provided vertex property is not a property of the host vertex: " + vertexProperty + " not a property of " + hostVertex);
        }

        public static IllegalStateException canNotAttachVertexPropertyToHostGraph(final Attachable<VertexProperty> vertexProperty, final Graph hostGraph) {
            return new IllegalStateException("The provided vertex property could not be found in the host graph: " + vertexProperty + " not in " + hostGraph);
        }

        public static IllegalStateException canNotAttachPropertyToHostVertex(final Attachable<Property> property, final Vertex hostVertex) {
            return new IllegalStateException("The provided property could not be attached the host vertex: " + property + " not a property in the star of " + hostVertex);
        }

        public static IllegalStateException canNotAttachPropertyToHostGraph(final Attachable<Property> property, final Graph hostGraph) {
            return new IllegalStateException("The provided property could not be attached the host graph: " + property + " not in " + hostGraph);
        }

        ////

        public static IllegalArgumentException illegalMethodOnHostVertex(final Attachable attachable, final Method method, final Vertex hostVertex) {
            return new IllegalArgumentException("The following method on the host vertex is not legal: " + hostVertex + "." + method + "(" + attachable + ")");
        }

        public static IllegalArgumentException illegalMethodOnHostGraph(final Attachable attachable, final Method method, final Graph hostGraph) {
            return new IllegalArgumentException("The following method on the host graph is not legal: " + hostGraph + "." + method + "(" + attachable + ")");
        }
    }

}