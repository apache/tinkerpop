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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGryoSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * The {@link GraphWriter} for the Gremlin Structure serialization format based on Kryo.  The format is meant to be
 * non-lossy in terms of Gremlin Structure to Gremlin Structure migrations (assuming both structure implementations
 * support the same graph features).
 * <p/>
 * This implementation is not thread-safe.  Have one {@code GraphWriter} instance per thread.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoWriter implements GraphWriter {
    private Kryo kryo;

    private GryoWriter(final Mapper<Kryo> gryoMapper) {
        this.kryo = gryoMapper.createMapper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        writeVertices(outputStream, g.vertices(), Direction.BOTH);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator, final Direction direction) throws IOException {
        kryo.getRegistration(StarGraph.class).setSerializer(StarGraphGryoSerializer.with(direction));
        final Output output = new Output(outputStream);
        while (vertexIterator.hasNext()) {
            writeVertexInternal(output, vertexIterator.next());
        }
        output.flush();
        kryo.getRegistration(StarGraph.class).setSerializer(StarGraphGryoSerializer.with(Direction.BOTH));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator) throws IOException {
        writeVertices(outputStream, vertexIterator, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        kryo.getRegistration(StarGraph.class).setSerializer(StarGraphGryoSerializer.with(direction));
        final Output output = new Output(outputStream);
        writeVertexInternal(output, v);
        output.flush();
        kryo.getRegistration(StarGraph.class).setSerializer(StarGraphGryoSerializer.with(Direction.BOTH));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        writeVertex(outputStream, v, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        kryo.writeObject(output, DetachedFactory.detach(e, true));
        output.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeVertexProperty(final OutputStream outputStream, final VertexProperty vp) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        kryo.writeObject(output, DetachedFactory.detach(vp, true));
        output.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeProperty(final OutputStream outputStream, final Property p) throws IOException {
        final Output output = new Output(outputStream);
        writeHeader(output);
        kryo.writeObject(output, DetachedFactory.detach(p, true));
        output.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeObject(final OutputStream outputStream, final Object object) {
        final Output output = new Output(outputStream);
        this.kryo.writeClassAndObject(output, object);
        output.flush();
    }

    void writeVertexInternal(final Output output, final Vertex v) throws IOException {
        writeHeader(output);
        kryo.writeObject(output, StarGraph.of(v));
        kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
    }

    void writeHeader(final Output output) throws IOException {
        output.writeBytes(GryoMapper.HEADER);
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder implements WriterBuilder<GryoWriter> {
        /**
         * Always creates the most current version available.
         */
        private Mapper<Kryo> gryoMapper = GryoMapper.build().create();

        private Builder() {
        }

        /**
         * Supply a mapper {@link GryoMapper} instance to use as the serializer for the {@code KryoWriter}.
         */
        public Builder mapper(final Mapper<Kryo> gryoMapper) {
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
