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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A @{link GraphReader} that constructs a graph from a JSON-based representation of a graph and its elements given
 * the "legacy" Blueprints 2.x version of GraphSON.  This implementation is specifically for aiding in migration
 * of graphs from TinkerPop 2.x to TinkerPop 3.x. This reader only reads GraphSON from TinkerPop 2.x that was
 * generated in {@code GraphSONMode.EXTENDED}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class LegacyGraphSONReader implements GraphReader {
    private final ObjectMapper mapper;
    private final long batchSize;

    private LegacyGraphSONReader(final ObjectMapper mapper, final long batchSize) {
        this.mapper = mapper;
        this.batchSize = batchSize;
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        final Map<Object,Vertex> cache = new HashMap<>();
        final AtomicLong counter = new AtomicLong(0);
        final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();
        final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
        final Graph.Features.VertexFeatures vertexFeatures = graphToWriteTo.features().vertex();

        final JsonFactory factory = mapper.getFactory();
        final LegacyGraphSONUtility graphson = new LegacyGraphSONUtility(graphToWriteTo, vertexFeatures, edgeFeatures, cache);

        try (JsonParser parser = factory.createParser(inputStream)) {
            if (parser.nextToken() != JsonToken.START_OBJECT)
                throw new IOException("Expected data to start with an Object");

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String fieldName = parser.getCurrentName() == null ? "" : parser.getCurrentName();
                switch (fieldName) {
                    case GraphSONTokensTP2.MODE:
                        parser.nextToken();
                        final String mode = parser.getText();
                        if (!mode.equals("EXTENDED"))
                            throw new IllegalStateException("The legacy GraphSON must be generated with GraphSONMode.EXTENDED");
                        break;
                    case GraphSONTokensTP2.VERTICES:
                        parser.nextToken();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            final JsonNode node = parser.readValueAsTree();
                            graphson.vertexFromJson(node);

                            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                                graphToWriteTo.tx().commit();
                        }
                        break;
                    case GraphSONTokensTP2.EDGES:
                        parser.nextToken();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            final JsonNode node = parser.readValueAsTree();
                            final Vertex inV = cache.get(LegacyGraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokensTP2._IN_V)));
                            final Vertex outV = cache.get(LegacyGraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokensTP2._OUT_V)));
                            graphson.edgeFromJson(node, outV, inV);

                            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                                graphToWriteTo.tx().commit();
                        }
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unexpected token in GraphSON - %s", fieldName));
                }
            }

            if (supportsTx) graphToWriteTo.tx().commit();
        } catch (Exception ex) {
            throw new IOException(ex);
        }

    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public Iterator<Vertex> readVertices(final InputStream inputStream,
                                         final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
                                         final Function<Attachable<Edge>, Edge> edgeAttachMethod,
                                         final Direction attachEdgesOfThisDirection) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
                             final Function<Attachable<Edge>, Edge> edgeAttachMethod,
                             final Direction attachEdgesOfThisDirection) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public VertexProperty readVertexProperty(final InputStream inputStream,
                                             final Function<Attachable<VertexProperty>, VertexProperty> vertexPropertyAttachMethod) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public Property readProperty(final InputStream inputStream,
                                 final Function<Attachable<Property>, Property> propertyAttachMethod) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this reader.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException {
        throw Io.Exceptions.readerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {
        private boolean loadCustomModules = false;
        private List<SimpleModule> customModules = new ArrayList<>();
        private long batchSize = 10000;
        private boolean embedTypes = false;

        private Builder() {
        }

        /**
         * Supply a mapper module for serialization/deserialization.
         */
        public Builder addCustomModule(final SimpleModule custom) {
            this.customModules.add(custom);
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #addCustomModule(com.fasterxml.jackson.databind.module.SimpleModule)};
         */
        public Builder loadCustomModules(final boolean loadCustomModules) {
            this.loadCustomModules = loadCustomModules;
            return this;
        }

        /**
         * Number of mutations to perform before a commit is executed.
         */
        public Builder batchSize(final long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public LegacyGraphSONReader create() {
            final GraphSONMapper.Builder builder = GraphSONMapper.build();
            customModules.forEach(builder::addCustomModule);
            final GraphSONMapper mapper = builder.embedTypes(embedTypes)
                    .loadCustomModules(loadCustomModules).create();
            return new LegacyGraphSONReader(mapper.createMapper(), batchSize);
        }
    }

}
