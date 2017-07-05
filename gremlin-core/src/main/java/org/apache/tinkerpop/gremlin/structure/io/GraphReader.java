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

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * Functions for reading a graph and its graph elements from a different serialization format. Implementations of
 * this class do not need to explicitly guarantee that an object read with one method must have its format
 * equivalent to another. In other words the input to {@link #readVertex(InputStream, Function)}} need not also
 * be readable by {@link #readObject(InputStream, Class)}. In other words, implementations are free
 * to optimize as is possible for a specific serialization method.
 * <p/>
 * That said, it is however important that the complementary "write" operation in {@link GraphWriter} be capable of
 * writing output compatible to its reader.  In other words, the output of
 * {@link GraphWriter#writeObject(OutputStream, Object)} should always be readable by
 * {@link #readObject(InputStream, Class)} and the output of {@link GraphWriter#writeGraph(OutputStream, Graph)}
 * should always be readable by {@link #readGraph(InputStream, Graph)}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphReader {

    /**
     * Reads an entire graph from an {@link InputStream}.  This method is mean to load an empty {@link Graph}.
     * It is up to individual implementations to manage transactions, but it is not required or enforced.  Consult
     * the documentation of an implementation to understand the approach it takes.
     *
     * @param inputStream    a stream containing an entire graph of vertices and edges as defined by the accompanying
     *                       {@link GraphWriter#writeGraph(OutputStream, Graph)}.
     * @param graphToWriteTo the graph to write to when reading from the stream.
     */
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException;

    /**
     * Reads a single vertex from an {@link InputStream}. This method will filter the read the read vertex by the provided
     * {@link GraphFilter}. If the graph filter will filter the vertex itself, then the returned {@link Optional} is empty.
     *
     * @param inputStream a stream containing at least a single vertex as defined by the accompanying
     *                    {@link GraphWriter#writeVertex(OutputStream, Vertex)}.
     * @param graphFilter The {@link GraphFilter} to filter the vertex and its associated edges by.
     * @return the vertex with filtered edges or {@link Optional#empty()}  if the vertex itself was filtered.
     * @throws IOException
     */
    public default Optional<Vertex> readVertex(final InputStream inputStream, final GraphFilter graphFilter) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " currently does not support " + GraphFilter.class.getSimpleName() + " deserialization filtering");
    }

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties but not edges.
     * It is expected that the user will manager their own transaction context with respect to this method (i.e.
     * implementations should not commit the transaction for the user).
     *
     * @param inputStream        a stream containing at least a single vertex as defined by the accompanying
     *                           {@link GraphWriter#writeVertex(OutputStream, Vertex)}.
     * @param vertexAttachMethod a function that creates re-attaches a {@link Vertex} to a {@link Host} object.
     */
    public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) throws IOException;

    /**
     * Reads a single vertex from an {@link InputStream}.  This method will read vertex properties as well as edges
     * given the direction supplied as an argument.  It is expected that the user will manager their own transaction
     * context with respect to this method (i.e. implementations should not commit the transaction for the user).
     *
     * @param inputStream                a stream containing at least one {@link Vertex} as defined by the accompanying
     *                                   {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)} method.
     * @param vertexAttachMethod         a function that creates re-attaches a {@link Vertex} to a {@link Host} object.
     * @param edgeAttachMethod           a function that creates re-attaches a {@link Edge} to a {@link Host} object.
     * @param attachEdgesOfThisDirection only edges of this direction are passed to the {@code edgeMaker}.
     */
    public Vertex readVertex(final InputStream inputStream,
                             final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
                             final Function<Attachable<Edge>, Edge> edgeAttachMethod,
                             final Direction attachEdgesOfThisDirection) throws IOException;

    /**
     * Reads a set of one or more vertices from an {@link InputStream} which were written by
     * {@link GraphWriter#writeVertices(OutputStream, Iterator)}.  This method will read vertex properties as well as
     * edges given the direction supplied as an argument. It is expected that the user will manager their own
     * transaction context with respect to this method (i.e. implementations should not commit the transaction for
     * the user).
     *
     * @param inputStream                a stream containing at least one {@link Vertex} as defined by the accompanying
     *                                   {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)} or
     *                                   {@link GraphWriter#writeVertices(OutputStream, Iterator)} methods.
     * @param vertexAttachMethod         a function that creates re-attaches a {@link Vertex} to a {@link Host} object.
     * @param edgeAttachMethod           a function that creates re-attaches a {@link Edge} to a {@link Host} object.
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
     * @param inputStream      a stream containing at least one {@link Edge} as defined by the accompanying
     *                         {@link GraphWriter#writeEdge(OutputStream, Edge)} method.
     * @param edgeAttachMethod a function that creates re-attaches a {@link Edge} to a {@link Host} object.
     */
    public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod) throws IOException;

    /**
     * Reads a single vertex property from an {@link InputStream}.  It is expected that the user will manager their own
     * transaction context with respect to this method (i.e. implementations should not commit the transaction for
     * the user).
     *
     * @param inputStream                a stream containing at least one {@link VertexProperty} as written by the accompanying
     *                                   {@link GraphWriter#writeVertexProperty(OutputStream, VertexProperty)} method.
     * @param vertexPropertyAttachMethod a function that creates re-attaches a {@link VertexProperty} to a
     *                                   {@link Host} object.
     * @return the value returned by the attach method.
     */
    public VertexProperty readVertexProperty(final InputStream inputStream,
                                             final Function<Attachable<VertexProperty>, VertexProperty> vertexPropertyAttachMethod) throws IOException;

    /**
     * Reads a single property from an {@link InputStream}.  It is expected that the user will manager their own
     * transaction context with respect to this method (i.e. implementations should not commit the transaction for
     * the user).
     *
     * @param inputStream          a stream containing at least one {@link Property} as written by the accompanying
     *                             {@link GraphWriter#writeProperty(OutputStream, Property)} method.
     * @param propertyAttachMethod a function that creates re-attaches a {@link Property} to a {@link Host} object.
     * @return the value returned by the attach method.
     */
    public Property readProperty(final InputStream inputStream,
                                 final Function<Attachable<Property>, Property> propertyAttachMethod) throws IOException;

    /**
     * Reads an arbitrary object using the registered serializers.
     *
     * @param inputStream a stream containing an object.
     * @param clazz       the class expected to be in the stream - may or may not be used by the underlying implementation.
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
