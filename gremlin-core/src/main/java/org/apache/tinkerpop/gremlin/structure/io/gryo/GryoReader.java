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

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

    private final File tempFile;

    private GryoReader(final File tempFile, final long batchSize,
                       final String vertexIdKey, final String edgeIdKey,
                       final GryoMapper gryoMapper) {
        this.kryo = gryoMapper.createMapper();
        this.vertexIdKey = vertexIdKey;
        this.edgeIdKey = edgeIdKey;
        this.tempFile = tempFile;
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<Vertex> readVertices(final InputStream inputStream, final Direction direction,
                                         final Function<DetachedVertex, Vertex> vertexMaker,
                                         final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        return new Iterator<Vertex>() {
            @Override
            public boolean hasNext() {
                return !input.eof();
            }

            @Override
            public Vertex next() {
                try {
                    final Vertex v = readVertex(direction, vertexMaker, edgeMaker, input);

                    // read the vertex terminator
                    kryo.readClassAndObject(input);

                    return v;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        readHeader(input);
        final Object o = kryo.readClassAndObject(input);
        return edgeMaker.apply((DetachedEdge) o);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<DetachedVertex, Vertex> vertexMaker) throws IOException {
        return readVertex(inputStream, null, vertexMaker, null);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction, Function<DetachedVertex, Vertex> vertexMaker, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Input input = new Input(inputStream);
        return readVertex(direction, vertexMaker, edgeMaker, input);
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        final Input input = new Input(inputStream);
        readHeader(input);

        final BatchGraph graph;
        try {
            // will throw an exception if not constructed properly
            graph = BatchGraph.build(graphToWriteTo)
                    .vertexIdKey(vertexIdKey)
                    .edgeIdKey(edgeIdKey)
                    .bufferSize(batchSize).create();
        } catch (Exception ex) {
            throw new IOException("Could not instantiate BatchGraph wrapper", ex);
        }

        try (final Output output = new Output(new FileOutputStream(tempFile))) {
            final boolean supportedMemory = input.readBoolean();
            if (supportedMemory) {
                // if the graph that serialized the data supported sideEffects then the sideEffects needs to be read
                // to advance the reader forward.  if the graph being read into doesn't support the sideEffects
                // then we just setting the data to sideEffects.
                final Map<String, Object> memMap = (Map<String, Object>) kryo.readObject(input, HashMap.class);
                if (graphToWriteTo.features().graph().variables().supportsVariables()) {
                    final Graph.Variables variables = graphToWriteTo.variables();
                    memMap.forEach(variables::set);
                }
            }

            final boolean hasSomeVertices = input.readBoolean();
            if (hasSomeVertices) {
                final List<Object> vertexArgs = new ArrayList<>();
                while (!input.eof()) {
                    final DetachedVertex current = (DetachedVertex) kryo.readClassAndObject(input);
                    appendToArgList(vertexArgs, T.id, current.id());
                    appendToArgList(vertexArgs, T.label, current.label());

                    final Vertex v = graph.addVertex(vertexArgs.toArray());
                    vertexArgs.clear();
                    current.properties().forEachRemaining(p -> createVertexProperty(graphToWriteTo, v, p));

                    // the gio file should have been written with a direction specified
                    final boolean hasDirectionSpecified = input.readBoolean();
                    final Direction directionInStream = kryo.readObject(input, Direction.class);
                    final Direction directionOfEdgeBatch = kryo.readObject(input, Direction.class);

                    // graph serialization requires that a direction be specified in the stream and that the
                    // direction of the edges be OUT
                    if (!hasDirectionSpecified || directionInStream != Direction.OUT || directionOfEdgeBatch != Direction.OUT)
                        throw new IllegalStateException(String.format("Stream must specify edge direction and that direction must be %s", Direction.OUT));

                    // if there are edges then read them to end and write to temp, otherwise read what should be
                    // the vertex terminator
                    if (!input.readBoolean())
                        kryo.readClassAndObject(input);
                    else
                        readToEndOfEdgesAndWriteToTemp(input, output);
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        // done writing to temp

        // start reading in the edges now from the temp file
        try (final Input edgeInput = new Input(new FileInputStream(tempFile))) {
            readFromTempEdges(edgeInput, graph);
            graph.tx().commit();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IOException(ex);
        } finally {
            deleteTempFileSilently();
        }
    }

    private static void createVertexProperty(final Graph graphToWriteTo, final Vertex v, final VertexProperty<Object> p) {
        final List<Object> propertyArgs = new ArrayList<>();
        if (graphToWriteTo.features().vertex().properties().supportsUserSuppliedIds())
            appendToArgList(propertyArgs, T.id, p.id());
        p.properties().forEachRemaining(it -> appendToArgList(propertyArgs, it.key(), it.value()));
        v.property(VertexProperty.Cardinality.list, p.key(), p.value(), propertyArgs.toArray());
    }

    private static void appendToArgList(final List<Object> propertyArgs, final Object key, final Object val) {
        propertyArgs.add(key);
        propertyArgs.add(val);
    }

    private Vertex readVertex(final Direction directionRequested, final Function<DetachedVertex, Vertex> vertexMaker,
                              final Function<DetachedEdge, Edge> edgeMaker, final Input input) throws IOException {
        if (null != directionRequested && null == edgeMaker)
            throw new IllegalArgumentException("If a directionRequested is specified then an edgeAdder function should also be specified");

        readHeader(input);

        final Vertex vertex = vertexMaker.apply((DetachedVertex) kryo.readClassAndObject(input));

        final boolean streamContainsEdgesInSomeDirection = input.readBoolean();
        if (!streamContainsEdgesInSomeDirection && directionRequested != null)
            throw new IllegalStateException(String.format("The direction %s was requested but no attempt was made to serialize edges into this stream", directionRequested));

        // if there are edges in the stream and the direction is not present then the rest of the stream is
        // simply ignored
        if (directionRequested != null) {
            final Direction directionsInStream = kryo.readObject(input, Direction.class);
            if (directionsInStream != Direction.BOTH && directionsInStream != directionRequested)
                throw new IllegalStateException(String.format("Stream contains %s edges, but requesting %s", directionsInStream, directionRequested));

            final Direction firstDirection = kryo.readObject(input, Direction.class);
            if (firstDirection == Direction.OUT && (directionRequested == Direction.BOTH || directionRequested == Direction.OUT))
                readEdges(input, edgeMaker);
            else {
                // requested direction in, but BOTH must be serialized so skip this.  the illegalstateexception
                // prior to this IF should  have caught a problem where IN is not supported at all
                if (firstDirection == Direction.OUT && directionRequested == Direction.IN)
                    skipEdges(input);
            }

            if (directionRequested == Direction.BOTH || directionRequested == Direction.IN) {
                // if the first direction was OUT then it was either read or skipped.  in that case, the marker
                // of the stream is currently ready to read the IN direction. otherwise it's in the perfect place
                // to start reading edges
                if (firstDirection == Direction.OUT)
                    kryo.readObject(input, Direction.class);

                readEdges(input, edgeMaker);
            }
        }

        return vertex;
    }

    private void readEdges(final Input input, final Function<DetachedEdge, Edge> edgeMaker) {
        if (input.readBoolean()) {
            Object next = kryo.readClassAndObject(input);
            while (!next.equals(EdgeTerminator.INSTANCE)) {
                edgeMaker.apply((DetachedEdge) next);
                next = kryo.readClassAndObject(input);
            }
        }
    }

    private void skipEdges(final Input input) {
        if (input.readBoolean()) {
            Object next = kryo.readClassAndObject(input);
            while (!next.equals(EdgeTerminator.INSTANCE)) {
                // next edge to skip or the terminator
                next = kryo.readClassAndObject(input);
            }
        }
    }

    private void readHeader(final Input input) throws IOException {
        if (!Arrays.equals(GryoMapper.GIO, input.readBytes(3)))
            throw new IOException("Invalid format - first three bytes of header do not match expected value");

        // skip the next 13 bytes - for future use
        input.readBytes(13);
    }

    /**
     * Reads through the all the edges for a vertex and writes the edges to a temp file which will be read later.
     */
    private void readToEndOfEdgesAndWriteToTemp(final Input input, final Output output) throws IOException {
        Object next = kryo.readClassAndObject(input);
        while (!next.equals(EdgeTerminator.INSTANCE)) {
            kryo.writeClassAndObject(output, next);

            // next edge or terminator
            next = kryo.readClassAndObject(input);
        }

        // this should be the vertex terminator
        kryo.readClassAndObject(input);

        kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
        kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
    }


    /**
     * Read the edges from the temp file and load them to the graph.
     */
    private void readFromTempEdges(final Input input, final Graph graphToWriteTo) {
        final List<Object> edgeArgs = new ArrayList<>();
        while (!input.eof()) {
            // in this case the outId is the id assigned by the graph
            Object next = kryo.readClassAndObject(input);
            while (!next.equals(EdgeTerminator.INSTANCE)) {
                final DetachedEdge detachedEdge = (DetachedEdge) next;
                final Vertex vOut = graphToWriteTo.vertices(detachedEdge.outVertex().id()).next();
                final Vertex inV = graphToWriteTo.vertices(detachedEdge.inVertex().id()).next();

                detachedEdge.properties().forEachRemaining(p -> edgeArgs.addAll(Arrays.asList(p.key(), p.value())));

                appendToArgList(edgeArgs, T.id, detachedEdge.id());

                vOut.addEdge(detachedEdge.label(), inV, edgeArgs.toArray());

                edgeArgs.clear();
                next = kryo.readClassAndObject(input);
            }

            // vertex terminator
            kryo.readClassAndObject(input);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteTempFileSilently() {
        try {
            tempFile.delete();
        } catch (Exception ignored) {
        }
    }

    public <T> T readObject(final InputStream inputStream) {
        return (T) this.kryo.readClassAndObject(new Input(inputStream));
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private File tempFile;
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;
        private String vertexIdKey = T.id.getAccessor();
        private String edgeIdKey = T.id.getAccessor();

        /**
         * Always use the most recent gryo version by default
         */
        private GryoMapper gryoMapper = GryoMapper.build().create();

        private Builder() {
            this.tempFile = new File(UUID.randomUUID() + ".tmp");
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

        /**
         * The reader requires a working directory to write temp files to.  If this value is not set, it will write
         * the temp file to the local directory.
         */
        public Builder workingDirectory(final String workingDirectory) {
            final File f = new File(workingDirectory);
            if (!f.exists() || !f.isDirectory())
                throw new IllegalArgumentException(String.format("%s is not a directory or does not exist", workingDirectory));

            tempFile = new File(workingDirectory + File.separator + UUID.randomUUID() + ".tmp");
            return this;
        }

        public GryoReader create() {
            return new GryoReader(tempFile, batchSize, this.vertexIdKey, this.edgeIdKey, this.gryoMapper);
        }
    }
}
