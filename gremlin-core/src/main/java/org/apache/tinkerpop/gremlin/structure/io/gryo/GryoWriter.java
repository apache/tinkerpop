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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The {@link GraphWriter} for the Gremlin Structure serialization format based on Kryo.  The format is meant to be
 * non-lossy in terms of Gremlin Structure to Gremlin Structure migrations (assuming both structure implementations
 * support the same graph features).
 * <br/>
 * This implementation is not thread-safe.  Have one {@code GraphWriter} instance per thread.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoWriter implements GraphWriter {
    private Kryo kryo;

    private GryoWriter(final GryoMapper gryoMapper) {
        this.kryo = gryoMapper.createMapper();
    }

    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);

        final boolean supportsGraphVariables = g.features().graph().variables().supportsVariables();
        output.writeBoolean(supportsGraphVariables);
        if (supportsGraphVariables)
            kryo.writeObject(output, new HashMap<>(g.variables().asMap()));

        final Iterator<Vertex> vertices = g.vertices();
        final boolean hasSomeVertices = vertices.hasNext();
        output.writeBoolean(hasSomeVertices);
        while (vertices.hasNext()) {
            final Vertex v = vertices.next();
            writeVertexToOutput(output, v, Direction.OUT);
        }

        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        writeVertexToOutput(output, v, direction);
        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        writeVertexWithNoEdgesToOutput(output, v);
        output.flush();
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        kryo.writeClassAndObject(output, DetachedFactory.detach(e, true));
        output.flush();
    }

    void writeHeader(final Output output) throws IOException {
        output.writeBytes(GryoMapper.HEADER);
    }

    private void writeEdgeToOutput(final Output output, final Edge e) {
        this.writeElement(output, e, null);
    }

    private void writeVertexWithNoEdgesToOutput(final Output output, final Vertex v) {
        writeElement(output, v, null);
    }

    private void writeVertexToOutput(final Output output, final Vertex v, final Direction direction) {
        this.writeElement(output, v, direction);
    }

    private void writeElement(final Output output, final Element e, final Direction direction) {
        kryo.writeClassAndObject(output, e);

        if (e instanceof Vertex) {
            output.writeBoolean(direction != null);
            if (direction != null) {
                final Vertex v = (Vertex) e;
                kryo.writeObject(output, direction);
                if (direction == Direction.BOTH || direction == Direction.OUT)
                    writeDirectionalEdges(output, Direction.OUT, v.edges(Direction.OUT));

                if (direction == Direction.BOTH || direction == Direction.IN)
                    writeDirectionalEdges(output, Direction.IN, v.edges(Direction.IN));
            }

            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }
    }

    private void writeDirectionalEdges(final Output output, final Direction d, final Iterator<Edge> vertexEdges) {
        final boolean hasEdges = vertexEdges.hasNext();
        kryo.writeObject(output, d);
        output.writeBoolean(hasEdges);

        while (vertexEdges.hasNext()) {
            final Edge edgeToWrite = vertexEdges.next();
            writeEdgeToOutput(output, edgeToWrite);
        }

        if (hasEdges)
            kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
    }

    public void writeObject(final OutputStream outputStream, final Object object) {
        final Output output = new Output(outputStream);
        this.kryo.writeClassAndObject(output, object);
        output.flush();
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Always creates the most current version available.
         */
        private GryoMapper gryoMapper = GryoMapper.build().create();

        private Builder() {
        }

        /**
         * Supply a mapper {@link GryoMapper} instance to use as the serializer for the {@code KryoWriter}.
         */
        public Builder mapper(final GryoMapper gryoMapper) {
            this.gryoMapper = gryoMapper;
            return this;
        }

        /**
         * Create the {@code GryoWriter}.
         */
        public GryoWriter create() {
            return new GryoWriter(this.gryoMapper);
        }
    }
}
