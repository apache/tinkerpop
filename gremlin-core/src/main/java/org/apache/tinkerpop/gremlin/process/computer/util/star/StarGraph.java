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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StarGraph implements Graph {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration();

    private StarVertex starVertex;

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Graph.Exceptions.vertexAdditionsNotSupported();
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
        return null == this.starVertex ?
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
                        .iterator();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return null == this.starVertex ?
                Collections.emptyIterator() :
                Stream.concat(
                        this.starVertex.inEdges.values().stream(),
                        this.starVertex.outEdges.values().stream())
                        .flatMap(List::stream)
                        .filter(edge -> ElementHelper.idExists(edge.id(), edgeIds))
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
        return EMPTY_CONFIGURATION;
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

    public static Vertex addTo(final StarGraph graph, final DetachedVertex detachedVertex) {
        if (null != graph.starVertex)
            return null;

        graph.starVertex = new StarVertex(detachedVertex.id(), detachedVertex.label());
        detachedVertex.properties().forEachRemaining(detachedVertexProperty -> {
            final VertexProperty<?> vertexProperty = graph.starVertex.property(VertexProperty.Cardinality.list, detachedVertexProperty.key(), detachedVertexProperty.value());
            detachedVertexProperty.properties().forEachRemaining(detachedVertexPropertyProperty -> {
                vertexProperty.property(detachedVertexPropertyProperty.key(), detachedVertexPropertyProperty.value());
                // todo: id of vertex property
            });
        });
        return graph.starVertex;
    }

    public static Edge addTo(final StarGraph graph, final DetachedEdge edge) {
        final Object id = edge.inVertex().id();
        if (!graph.starVertex.id().equals(id)) {
            final Edge outEdge = graph.starVertex.addEdge(edge.label(), new StarInVertex(edge.inVertex().id(), graph.starVertex), new Object[]{T.id, edge.id()});
            edge.properties().forEachRemaining(property -> outEdge.property(property.key(), property.value()));
            return outEdge;
        } else {
            final Edge inEdge = new StarOutVertex(edge.outVertex().id(), graph.starVertex).addEdge(edge.label(), graph.starVertex, new Object[]{T.id, edge.id()});
            edge.properties().forEachRemaining(property -> inEdge.property(property.key(), property.value()));
            return inEdge;
        }
    }

    protected static Long randomId() {
        return new Random().nextLong(); // TODO: you shouldn't need this!
    }
}
