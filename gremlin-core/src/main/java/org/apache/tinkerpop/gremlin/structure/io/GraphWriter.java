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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Functions for writing a graph and its elements to a serialized format. Implementations of this class do not need
 * to explicitly guarantee that an object written with one method must have its format equivalent to another. In other
 * words, calling {@link #writeVertex(OutputStream, Vertex)}} need not have equivalent output to
 * {@link #writeObject(OutputStream, Object)}.  Nor does the representation of an {@link Edge} within the output of
 * {@link #writeVertex(OutputStream, Vertex, Direction)} need to match the representation of that same
 * {@link Edge} when provided to {@link #writeEdge(OutputStream, Edge)}. In other words, implementations are free
 * to optimize as is possible for a specific serialization method.
 * <p/>
 * That said, it is however important that the complementary "read" operation in {@link GraphReader} be capable of
 * reading the output of the writer.  In other words, the output of {@link #writeObject(OutputStream, Object)}
 * should always be readable by {@link GraphReader#readObject(InputStream, Class)} and the output of
 * {@link #writeGraph(OutputStream, Graph)} should always be readable by
 * {@link GraphReader#readGraph(InputStream, Graph)}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {
    /**
     * Write the entire graph to a stream.
     *
     * @param outputStream the stream to write to.
     * @param g the graph to write to stream.
     */
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException;

    /**
     * Write a vertex to a stream with its associated edges.  Only write edges as defined by the requested direction.
     *
     * @param outputStream the stream to write to.
     * @param v            the vertex to write.
     * @param direction    the direction of edges to write or null if no edges are to be written.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException;

    /**
     * Write a vertex to a stream without writing its edges.
     *
     * @param outputStream the stream to write to.
     * @param v            the vertex to write.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException;


    /**
     * Write a list of vertices from a {@link Traversal} to a stream with its associated edges.  Only write edges as
     * defined by the requested direction.
     *
     * @param outputStream the stream to write to.
     * @param vertexIterator a traversal that returns a list of vertices.
     * @param direction the direction of edges to write or null if no edges are to be written.
     */
    public default void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator, final Direction direction) throws IOException {
        while (vertexIterator.hasNext()) {
            writeVertex(outputStream, vertexIterator.next(), direction);
        }
    }

    /**
     * Write a vertex to a stream without writing its edges.
     *
     * @param outputStream the stream to write to.
     * @param vertexIterator a iterator that returns a list of vertices.
     */
    public default void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator) throws IOException {
        while (vertexIterator.hasNext()) {
            writeVertex(outputStream, vertexIterator.next());
        }
    }

    /**
     * Write an edge to a stream.
     *
     * @param outputStream the stream to write to.
     * @param e the edge to write.
     */
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException;

    /**
     * Write a vertex property to a stream.
     *
     * @param outputStream the stream to write to.
     * @param vp the vertex property to write.
     */
    public void writeVertexProperty(final OutputStream outputStream, final VertexProperty vp) throws IOException;

    /**
     * Write a property to a stream.
     *
     * @param outputStream the stream to write to.
     * @param p the property to write.
     */
    public void writeProperty(final OutputStream outputStream, final Property p) throws IOException;

    /**
     * Writes an arbitrary object to the stream.
     *
     * @param outputStream the stream to write to.
     * @param object the object to write which will use the standard serializer set.
     */
    public void writeObject(final OutputStream outputStream, final Object object) throws IOException;

    /**
     * Largely a marker interface for builder classes that construct a {@link GraphWriter}.
     */
    public interface WriterBuilder<T extends GraphWriter> {
        /**
         * Creates the {@link GraphWriter} implementation given options provided to the {@link WriterBuilder}
         * implementation.
         */
        T create();
    }
}
