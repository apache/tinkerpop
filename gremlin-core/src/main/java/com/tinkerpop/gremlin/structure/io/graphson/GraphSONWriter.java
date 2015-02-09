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
package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;

import java.io.*;

/**
 * A @{link GraphWriter} that writes a graph and its elements to a JSON-based representation. This implementation
 * only supports JSON data types and is therefore lossy with respect to data types (e.g. a float will become a double).
 * Further note that serialized {@code Map} objects do not support complex types for keys.  {@link Edge} and
 * {@link Vertex} objects are serialized to {@code Map} instances. If an
 * {@link com.tinkerpop.gremlin.structure.Element} is used as a key, it is coerced to its identifier.  Other complex
 * objects are converted via {@link Object#toString()} unless a mapper serializer is supplied.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONWriter implements GraphWriter {
    private final ObjectMapper mapper;

    private GraphSONWriter(final GraphSONMapper mapper) {
        this.mapper = mapper.createMapper();
    }

    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        this.mapper.writeValue(outputStream, new GraphSONGraph(g));
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        this.mapper.writeValue(outputStream, new GraphSONVertex(v, direction));
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        this.mapper.writeValue(outputStream, v);
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        this.mapper.writeValue(outputStream, e);
    }

    @Override
    public void writeVertices(final OutputStream outputStream, final Traversal<?, Vertex> traversal, final Direction direction) throws IOException {
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            while (traversal.hasNext()) {
                writeVertex(baos, traversal.next(), direction);
                writer.write(new String(baos.toByteArray()));
                writer.newLine();
                baos.reset();
            }
        }

        writer.flush();
    }

    @Override
    public void writeVertices(final OutputStream outputStream, final Traversal<?, Vertex> traversal) throws IOException {
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            while (traversal.hasNext()) {
                writeVertex(baos, traversal.next());
                writer.write(new String(baos.toByteArray()));
                writer.newLine();
                baos.reset();
            }
        }

        writer.flush();
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

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
