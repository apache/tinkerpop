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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializer;

import java.io.*;
import java.util.Iterator;

/**
 * A @{link GraphWriter} that writes a graph and its elements to a JSON-based representation. This implementation
 * only supports JSON data types and is therefore lossy with respect to data types (e.g. a float will become a double).
 * Further note that serialized {@code Map} objects do not support complex types for keys.  {@link Edge} and
 * {@link Vertex} objects are serialized to {@code Map} instances. If an
 * {@link org.apache.tinkerpop.gremlin.structure.Element} is used as a key, it is coerced to its identifier.  Other complex
 * objects are converted via {@link Object#toString()} unless a mapper serializer is supplied.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONWriter implements GraphWriter {
    private final ObjectMapper mapper;

    private GraphSONWriter(final GraphSONMapper mapper) {
        this.mapper = mapper.createMapper();
    }

    /**
     * Writes a {@link Graph} to stream in an adjacency list format where vertices are written with edges from both
     * directions.  Under this serialization model, edges are grouped by label.
     *
     * @param outputStream the stream to write to.
     * @param g the graph to write to stream.
     */
    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        writeVertices(outputStream, g.vertices(), Direction.BOTH);
    }

    /**
     * Writes a single {@link Vertex} to stream where edges only from the specified direction are written.
     * Under this serialization model, edges are grouped by label.
     *
     * @param direction the direction of edges to write or null if no edges are to be written.
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        mapper.writeValue(outputStream, new StarGraphGraphSONSerializer.DirectionalStarGraph(StarGraph.of(v), direction));
    }

    /**
     * Writes a single {@link Vertex} with no edges serialized.
     *
     * @param outputStream the stream to write to.
     * @param v            the vertex to write.
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        writeVertex(outputStream, v, null);
    }

    /**
     * Writes a list of vertices in adjacency list format where vertices are written with edges from both
     * directions.  Under this serialization model, edges are grouped by label.
     *
     * @param outputStream the stream to write to.
     * @param vertexIterator    a traversal that returns a list of vertices.
     * @param direction    if direction is null then no edges are written.
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator, final Direction direction) throws IOException {
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            while (vertexIterator.hasNext()) {
                writeVertex(baos, vertexIterator.next(), direction);
                writer.write(new String(baos.toByteArray()));
                writer.newLine();
                baos.reset();
            }
        }

        writer.flush();
    }

    /**
     * Writes a list of vertices without edges.
     *
     * @param outputStream the stream to write to.
     * @param vertexIterator    a iterator that returns a list of vertices.
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator) throws IOException {
        writeVertices(outputStream, vertexIterator, null);
    }

    /**
     * Writes an {@link Edge} object to the stream.  Note that this format is different from the format of an
     * {@link Edge} when serialized with a {@link Vertex} as done with
     * {@link #writeVertex(OutputStream, Vertex, Direction)} or
     * {@link #writeVertices(OutputStream, Iterator, Direction)} in that the edge label is part of the object and
     * vertex labels are included with their identifiers.
     *
     * @param outputStream the stream to write to.
     * @param e the edge to write.
     */
    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        mapper.writeValue(outputStream, e);
    }

    /**
     * Write a {@link VertexProperty} object to the stream.
     *
     * @param outputStream the stream to write to.
     * @param vp the vertex property to write.
     */
    @Override
    public void writeVertexProperty(final OutputStream outputStream, final VertexProperty vp) throws IOException {
        mapper.writeValue(outputStream, vp);
    }

    /**
     * Write a {@link Property} object to the stream.
     *
     * @param outputStream the stream to write to.
     * @param p the property to write.
     */
    @Override
    public void writeProperty(final OutputStream outputStream, final Property p) throws IOException {
        mapper.writeValue(outputStream, p);
    }

    /**
     * Writes an arbitrary object to the stream.  Note that Gremlin Server uses this method when serializing output,
     * thus the format of the GraphSON for a {@link Vertex} will be somewhat different from the format supplied
     * when using {@link #writeVertex(OutputStream, Vertex, Direction)}. For example, edges will never be included.
     *
     * @param outputStream the stream to write to
     * @param object the object to write which will use the standard serializer set
     */
    @Override
    public void writeObject(final OutputStream outputStream, final Object object) throws IOException {
        this.mapper.writeValue(outputStream, object);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder implements WriterBuilder<GraphSONWriter> {

        private GraphSONMapper mapper = GraphSONMapper.build().create();

        private Builder() {
        }

        /**
         * Override all of the builder options with this mapper.  If this value is set to something other than
         * null then that value will be used to construct the writer.
         */
        public Builder mapper(final GraphSONMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public GraphSONWriter create() {
            return new GraphSONWriter(mapper);
        }
    }
}
