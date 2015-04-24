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

import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.shaded.kryo.io.Input;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@link GraphReader} for the Gremlin Structure serialization format based on Kryo.  The format is meant to be
 * non-lossy in terms of Gremlin Structure to Gremlin Structure migrations (assuming both structure implementations
 * support the same graph features).
 * <br/>
 * This implementation is not thread-safe.  Have one {@code GryoReader} instance per thread.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoReader implements GraphReader {
    private final Kryo kryo;

    private final long batchSize;
    private final String vertexIdKey;
    private final String edgeIdKey;

    private GryoReader(final long batchSize,
                       final String vertexIdKey, final String edgeIdKey,
                       final GryoMapper gryoMapper) {
        this.kryo = gryoMapper.createMapper();
        this.vertexIdKey = vertexIdKey;
        this.edgeIdKey = edgeIdKey;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<Vertex> readVertices(final InputStream inputStream,
                                         final Function<Attachable<Vertex>, Vertex> vertexMaker,
                                         final Function<Attachable<Edge>, Edge> edgeMaker) throws IOException {
        return new VertexInputIterator(new Input(inputStream), vertexMaker);
    }

    private class VertexInputIterator implements Iterator<Vertex> {
        private final Input input;
        private final Function<Attachable<Vertex>, Vertex> vertexMaker;

        public VertexInputIterator(final Input input, final Function<Attachable<Vertex>, Vertex> vertexMaker) {
            this.input = input;
            this.vertexMaker = vertexMaker;
        }

        @Override
        public boolean hasNext() {
            return !input.eof();
        }

        @Override
        public Vertex next() {
            try {
                return readVertexInternal(vertexMaker, input);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        readHeader(input);
        final Attachable<Edge> attachable = kryo.readObject(input, DetachedEdge.class);
        return edgeMaker.apply(attachable);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexMaker) throws IOException {
        return readVertex(inputStream, vertexMaker, null);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream,
                             final Function<Attachable<Vertex>, Vertex> vertexMaker,
                             final Function<Attachable<Edge>, Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        return readVertexInternal(vertexMaker, input);
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        // dual pass - create all vertices and store to cache the ids.  then create edges.  as long as we don't
        // have vertex labels in the output we can't do this single pass
        final Map<Vertex,Vertex> cache = new HashMap<>();
        IteratorUtils.iterate(new VertexInputIterator(new Input(inputStream), attachable ->
                cache.put(attachable.get(), attachable.attach(graphToWriteTo, Attachable.Method.CREATE))));
        cache.entrySet().forEach(kv -> kv.getKey().edges(Direction.OUT)
                .forEachRemaining(e -> ((Attachable) e).attach(kv.getValue(), Attachable.Method.CREATE)));
    }

    @Override
    public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException{
        return clazz.cast(this.kryo.readClassAndObject(new Input(inputStream)));
    }

    private Vertex readVertexInternal(final Function<Attachable<Vertex>, Vertex> vertexMaker, final Input input) throws IOException {
        readHeader(input);
        final StarGraph starGraph = kryo.readObject(input, StarGraph.class);

        // read the terminator
        kryo.readClassAndObject(input);

        return vertexMaker.apply(starGraph.getStarVertex());
    }

    private void readHeader(final Input input) throws IOException {
        if (!Arrays.equals(GryoMapper.GIO, input.readBytes(3)))
            throw new IOException("Invalid format - first three bytes of header do not match expected value");

        // skip the next 13 bytes - for future use
        input.readBytes(13);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder implements ReaderBuilder<GryoReader> {
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;
        private String vertexIdKey = T.id.getAccessor();
        private String edgeIdKey = T.id.getAccessor();

        /**
         * Always use the most recent gryo version by default
         */
        private GryoMapper gryoMapper = GryoMapper.build().create();

        private Builder() {
        }

        /**
         * Set the size between commits when reading into the {@link Graph} instance.  This value defaults to
         * {@link BatchGraph#DEFAULT_BUFFER_SIZE}.
         */
        public Builder batchSize(final long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Supply a mapper {@link GryoMapper} instance to use as the serializer for the {@code KryoWriter}.
         */
        public Builder mapper(final GryoMapper gryoMapper) {
            this.gryoMapper = gryoMapper;
            return this;
        }

        /**
         * The name of the key to supply to
         * {@link org.apache.tinkerpop.gremlin.structure.util.batch.BatchGraph.Builder#vertexIdKey} when reading data into
         * the {@link Graph}.
         */
        public Builder vertexIdKey(final String vertexIdKey) {
            this.vertexIdKey = vertexIdKey;
            return this;
        }

        /**
         * The name of the key to supply to
         * {@link org.apache.tinkerpop.gremlin.structure.util.batch.BatchGraph.Builder#edgeIdKey} when reading data into
         * the {@link Graph}.
         */
        public Builder edgeIdKey(final String edgeIdKey) {
            this.edgeIdKey = edgeIdKey;
            return this;
        }

        public GryoReader create() {
            return new GryoReader(batchSize, this.vertexIdKey, this.edgeIdKey, this.gryoMapper);
        }
    }
}
