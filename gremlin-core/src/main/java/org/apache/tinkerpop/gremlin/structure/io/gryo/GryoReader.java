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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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

    private GryoReader(final long batchSize, final GryoMapper gryoMapper) {
        this.kryo = gryoMapper.createMapper();
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<Vertex> readVertices(final InputStream inputStream,
                                         final Function<Attachable<Vertex>, Vertex> vertexMaker,
                                         final Function<Attachable<Edge>, Edge> edgeMaker,
                                         final Direction attachEdgesOfThisDirection) throws IOException {
        return new VertexInputIterator(new Input(inputStream), vertexMaker, attachEdgesOfThisDirection, edgeMaker);
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
        return readVertex(inputStream, vertexMaker, null, null);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream,
                             final Function<Attachable<Vertex>, Vertex> vertexMaker,
                             final Function<Attachable<Edge>, Edge> edgeMaker,
                             final Direction attachEdgesOfThisDirection) throws IOException {
        final Input input = new Input(inputStream);
        return readVertexInternal(vertexMaker, edgeMaker, attachEdgesOfThisDirection, input);
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        // dual pass - create all vertices and store to cache the ids.  then create edges.  as long as we don't
        // have vertex labels in the output we can't do this single pass
        final Map<StarGraph.StarVertex,Vertex> cache = new HashMap<>();
        final AtomicLong counter = new AtomicLong(0);
        final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();
        IteratorUtils.iterate(new VertexInputIterator(new Input(inputStream), attachable -> {
            final Vertex v = cache.put((StarGraph.StarVertex) attachable.get(), attachable.attach(Attachable.Method.create(graphToWriteTo)));
            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                graphToWriteTo.tx().commit();
            return v;
        }, null, null));
        cache.entrySet().forEach(kv -> kv.getKey().edges(Direction.OUT).forEachRemaining(e -> {
            ((StarGraph.StarEdge) e).attach(Attachable.Method.create(kv.getValue()));
            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                graphToWriteTo.tx().commit();
        }));
    }

    @Override
    public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException{
        return clazz.cast(this.kryo.readClassAndObject(new Input(inputStream)));
    }

    private Vertex readVertexInternal(final Function<Attachable<Vertex>, Vertex> vertexMaker,
                                      final Function<Attachable<Edge>, Edge> edgeMaker,
                                      final Direction d,
                                      final Input input) throws IOException {
        readHeader(input);
        final StarGraph starGraph = kryo.readObject(input, StarGraph.class);

        // read the terminator
        kryo.readClassAndObject(input);

        final Vertex v = vertexMaker.apply(starGraph.getStarVertex());
        if (edgeMaker != null) starGraph.getStarVertex().edges(d).forEachRemaining(e -> edgeMaker.apply((Attachable<Edge>) e));
        return v;
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

        public GryoReader create() {
            return new GryoReader(batchSize, this.gryoMapper);
        }

    }

    private class VertexInputIterator implements Iterator<Vertex> {
        private final Input input;
        private final Function<Attachable<Vertex>, Vertex> vertexMaker;
        private final Direction d;
        private final Function<Attachable<Edge>, Edge> edgeMaker;

        public VertexInputIterator(final Input input,
                                   final Function<Attachable<Vertex>, Vertex> vertexMaker,
                                   final Direction d,
                                   final Function<Attachable<Edge>, Edge> edgeMaker) {
            this.input = input;
            this.d = d;
            this.edgeMaker = edgeMaker;
            this.vertexMaker = vertexMaker;
        }

        @Override
        public boolean hasNext() {
            return !input.eof();
        }

        @Override
        public Vertex next() {
            try {
                return readVertexInternal(vertexMaker, edgeMaker, d, input);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
