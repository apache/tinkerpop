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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Functions for reading a graph and its graph elements from a different format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphReader {

    /**
     * Reads an entire graph from an {@link InputStream}.  This method is mean to load an empty {@link Graph}.
     * It is up to individual implementations to manage transactions, but it is not required or enforced.  Consult
     * the documentation of an implementation to understand the approach it takes.
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     */
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException;

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties but not edges.
     * It is expected that the user will manager their own transaction context with respect to this method (i.e.
     * implementations should not commit the transaction for the user).
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param vertexAttachMethod a function to create a vertex where the first argument is the vertex identifier, the
     *                    second argument is vertex label and the last is the list of properties for it
     */
    public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) throws IOException;

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties as well as edges
     * given the direction supplied as an argument.  It is expected that the user will manager their own transaction
     * context with respect to this method (i.e. implementations should not commit the transaction for the user).
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param vertexAttachMethod a function to create a vertex where the first argument is the vertex identifier, the
     *                    second argument is vertex label and the last is the list of properties for it
     * @param edgeAttachMethod   a function that creates an edge from the stream where the first argument is the edge
     *                    identifier, the second argument is the out vertex id, the third is the in vertex id,
     *                    the fourth is the label, and the fifth is the list of properties as key/value pairs.
     * @param attachEdgesOfThisDirection only edges of this direction are passed to the {@code edgeMaker}.
     */
    public Vertex readVertex(final InputStream inputStream,
                             final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
                             final Function<Attachable<Edge>, Edge> edgeAttachMethod,
                             final Direction attachEdgesOfThisDirection) throws IOException;

    /**
     * Reads a set of vertices from an {@link InputStream} which were written by
     * {@link GraphWriter#writeVertices(OutputStream, Iterator)}.  This method will read vertex properties as well as
     * edges given the direction supplied as an argument. It is expected that the user will manager their own
     * transaction context with respect to this method (i.e. implementations should not commit the transaction for
     * the user).
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param vertexAttachMethod a function to create a vertex where the first argument is the vertex identifier, the
     *                    second argument is vertex label and the last is the list of properties for it
     * @param edgeAttachMethod   a function that creates an edge from the stream where the first argument is the edge
     *                    identifier, the second argument is the out vertex id, the third is the in vertex id,
     *                    the fourth is the label, and the fifth is the list of properties as key/value pairs.
     * @param attachEdgesOfThisDirection only edges of this direction are passed to the {@code edgeMaker}.
     */
    public Iterator<Vertex> readVertices(final InputStream inputStream,
                                         final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
                                         final Function<Attachable<Edge>, Edge> edgeAttachMethod,
                                         final Direction attachEdgesOfThisDirection) throws IOException;

    /**
     * Reads a single edge from an {@link InputStream}. It is expected that the user will manager their own
     * transaction context with respect to this method (i.e. implementations should not commit the transaction for
     * the user).
     *
     * @param inputStream a stream containing a single vertex as defined by the accompanying {@link GraphWriter}
     * @param edgeAttachMethod    a function that creates an edge from the stream where the first argument is the edge
     *                    identifier, the second argument is the out vertex id, the third is the in vertex id,
     *                    the fourth is the label, and the fifth is the list of properties as key/value pairs.
     */
    public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod) throws IOException;

    /**
     * Reads an arbitrary object using the standard serializers.
     *
     * @param inputStream  a stream containing an object.
     */
    public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException;

    /**
     * Largely a marker interface for builder classes that construct a {@link GraphReader}.
     */
    public interface ReaderBuilder<T extends GraphReader> {
        /**
         * Creates the {@link GraphReader} implementation given options provided to the {@link ReaderBuilder}
         * implementation.
         */
        T create();
    }
}
