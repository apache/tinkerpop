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
package com.tinkerpop.gremlin.structure.io;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Functions for writing a graph and its elements to a different format.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphWriter {
    /**
     * Write the entire graph to a stream.
     */
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException;

    /**
     * Write a vertex to a stream with its associated edges.  Only write edges as defined by the requested direction.
     *
     * @param outputStream The stream to write to.
     * @param v            The vertex to write.
     * @param direction    If direction is null then no edges are written.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException;

    /**
     * Write a vertex to a stream without writing its edges.
     *
     * @param outputStream The stream to write to.
     * @param v            The vertex to write.
     */
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException;


    /**
     * Write a list of vertices from a {@link Traversal} to a stream with its associated edges.  Only write edges as
     * defined by the requested direction.
     *
     * @param outputStream The stream to write to.
     * @param traversal    A traversal that returns a list of vertices.
     * @param direction    If direction is null then no edges are written.
     */
    public default void writeVertices(final OutputStream outputStream, final Traversal<?, Vertex> traversal, final Direction direction) throws IOException {
        while (traversal.hasNext()) {
            writeVertex(outputStream, traversal.next(), direction);
        }
    }

    /**
     * Write a vertex to a stream without writing its edges.
     *
     * @param outputStream The stream to write to.
     * @param traversal    A traversal that returns a list of vertices.
     */
    public default void writeVertices(final OutputStream outputStream, final Traversal<?, Vertex> traversal) throws IOException {
        while (traversal.hasNext()) {
            writeVertex(outputStream, traversal.next());
        }
    }

    /**
     * Write an edge to a stream.
     */
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException;
}
