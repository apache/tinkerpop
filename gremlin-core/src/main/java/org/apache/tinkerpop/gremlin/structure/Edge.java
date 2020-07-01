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
package org.apache.tinkerpop.gremlin.structure;

import java.util.Iterator;

/**
 * An {@link Edge} links two {@link Vertex} objects. Along with its {@link Property} objects, an {@link Edge} has both
 * a {@link Direction} and a {@code label}. The {@link Direction} determines which {@link Vertex} is the tail
 * {@link Vertex} (out {@link Vertex}) and which {@link Vertex} is the head {@link Vertex}
 * (in {@link Vertex}). The {@link Edge} {@code label} determines the type of relationship that exists between the
 * two vertices.
 * <p/>
 * Diagrammatically:
 * <pre>
 * outVertex ---label---&gt; inVertex.
 * </pre>
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Edge extends Element {

    /**
     * The default label to use for an edge.
     * This is typically never used as when an edge is created, an edge label is required to be specified.
     */
    public static final String DEFAULT_LABEL = "edge";

    /**
     * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
     * If the direction is {@link Direction#BOTH} then the iterator order is: {@link Direction#OUT} then {@link Direction#IN}.
     *
     * @param direction Get the incoming vertex, outgoing vertex, or both vertices
     * @return An iterator with 1 or 2 vertices
     */
    public Iterator<Vertex> vertices(final Direction direction);

    /**
     * Get the outgoing/tail vertex of this edge.
     *
     * @return the outgoing vertex of the edge
     */
    public default Vertex outVertex() {
        return this.vertices(Direction.OUT).next();
    }

    /**
     * Get the incoming/head vertex of this edge.
     *
     * @return the incoming vertex of the edge
     */
    public default Vertex inVertex() {
        return this.vertices(Direction.IN).next();
    }

    /**
     * Get both the outgoing and incoming vertices of this edge.
     * The first vertex in the iterator is the outgoing vertex.
     * The second vertex in the iterator is the incoming vertex.
     *
     * @return an iterator of the two vertices of this edge
     */
    public default Iterator<Vertex> bothVertices() {
        return this.vertices(Direction.BOTH);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys);


    /**
     * Common exceptions to use with an edge.
     */
    public static class Exceptions extends Element.Exceptions {

        private Exceptions() {
        }

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers of this type");
        }

        public static IllegalStateException edgeRemovalNotSupported() {
            return new IllegalStateException("Edge removal are not supported");
        }
    }
}
