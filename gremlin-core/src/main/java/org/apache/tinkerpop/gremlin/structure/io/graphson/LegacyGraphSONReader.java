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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

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
         * the one supplied to the {@link #addCustomModule(SimpleModule)};
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
            final GraphSONMapper mapper = builder.typeInfo(embedTypes ? TypeInfo.PARTIAL_TYPES : TypeInfo.NO_TYPES)
                    .loadCustomModules(loadCustomModules).create();
            return new LegacyGraphSONReader(mapper.createMapper(), batchSize);
        }
    }

    static class LegacyGraphSONUtility {
        private static final String EMPTY_STRING = "";
        private final Graph g;
        private final Graph.Features.VertexFeatures vertexFeatures;
        private final Graph.Features.EdgeFeatures edgeFeatures;
        private final Map<Object,Vertex> cache;

        public LegacyGraphSONUtility(final Graph g, final Graph.Features.VertexFeatures vertexFeatures,
                                     final Graph.Features.EdgeFeatures edgeFeatures,
                                     final Map<Object, Vertex> cache) {
            this.g = g;
            this.vertexFeatures = vertexFeatures;
            this.edgeFeatures = edgeFeatures;
            this.cache = cache;
        }

        public Vertex vertexFromJson(final JsonNode json) throws IOException {
            final Map<String, Object> props = readProperties(json);

            final Object vertexId = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));
            final Vertex v = vertexFeatures.willAllowId(vertexId) ? g.addVertex(T.id, vertexId) : g.addVertex();
            cache.put(vertexId, v);

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                v.property(g.features().vertex().getCardinality(entry.getKey()), entry.getKey(), entry.getValue());
            }

            return v;
        }

        public Edge edgeFromJson(final JsonNode json, final Vertex out, final Vertex in) throws IOException {
            final Map<String, Object> props = LegacyGraphSONUtility.readProperties(json);

            final Object edgeId = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));
            final JsonNode nodeLabel = json.get(GraphSONTokensTP2._LABEL);
            final String label = nodeLabel == null ? EMPTY_STRING : nodeLabel.textValue();

            final Edge e = edgeFeatures.willAllowId(edgeId) ? out.addEdge(label, in, T.id, edgeId) : out.addEdge(label, in) ;
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                e.property(entry.getKey(), entry.getValue());
            }

            return e;
        }

        static Map<String, Object> readProperties(final JsonNode node) {
            final Map<String, Object> map = new HashMap<>();

            final Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                final Map.Entry<String, JsonNode> entry = iterator.next();

                if (!isReservedKey(entry.getKey())) {
                    // it generally shouldn't be as such but graphson containing null values can't be shoved into
                    // element property keys or it will result in error
                    final Object o = readProperty(entry.getValue());
                    if (o != null) {
                        map.put(entry.getKey(), o);
                    }
                }
            }

            return map;
        }

        private static boolean isReservedKey(final String key) {
            return key.equals(GraphSONTokensTP2._ID) || key.equals(GraphSONTokensTP2._TYPE) || key.equals(GraphSONTokensTP2._LABEL)
                    || key.equals(GraphSONTokensTP2._OUT_V) || key.equals(GraphSONTokensTP2._IN_V);
        }

        private static Object readProperty(final JsonNode node) {
            final Object propertyValue;

            if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_UNKNOWN)) {
                propertyValue = null;
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BOOLEAN)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).booleanValue();
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_FLOAT)) {
                propertyValue = Float.parseFloat(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BYTE)) {
                propertyValue = Byte.parseByte(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_SHORT)) {
                propertyValue = Short.parseShort(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_DOUBLE)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).doubleValue();
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_INTEGER)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).intValue();
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LONG)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).longValue();
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_STRING)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).textValue();
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LIST)) {
                propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE).elements());
            } else if (node.get(GraphSONTokensTP2.TYPE).textValue().equals(GraphSONTokensTP2.TYPE_MAP)) {
                propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE));
            } else {
                propertyValue = node.textValue();
            }

            return propertyValue;
        }

        private static List readProperties(final Iterator<JsonNode> listOfNodes) {
            final List<Object> array = new ArrayList<>();

            while (listOfNodes.hasNext()) {
                array.add(readProperty(listOfNodes.next()));
            }

            return array;
        }

        static Object getTypedValueFromJsonNode(final JsonNode node) {
            Object theValue = null;

            if (node != null && !node.isNull()) {
                if (node.isBoolean()) {
                    theValue = node.booleanValue();
                } else if (node.isDouble()) {
                    theValue = node.doubleValue();
                } else if (node.isFloatingPointNumber()) {
                    theValue = node.floatValue();
                } else if (node.isInt()) {
                    theValue = node.intValue();
                } else if (node.isLong()) {
                    theValue = node.longValue();
                } else if (node.isTextual()) {
                    theValue = node.textValue();
                } else if (node.isArray()) {
                    // this is an array so just send it back so that it can be
                    // reprocessed to its primitive components
                    theValue = node;
                } else if (node.isObject()) {
                    // this is an object so just send it back so that it can be
                    // reprocessed to its primitive components
                    theValue = node;
                } else {
                    theValue = node.textValue();
                }
            }

            return theValue;
        }
    }

    public final static class GraphSONTokensTP2 {

        private GraphSONTokensTP2() {}

        public static final String _ID = "_id";
        public static final String _LABEL = "_label";
        public static final String _TYPE = "_type";
        public static final String _OUT_V = "_outV";
        public static final String _IN_V = "_inV";
        public static final String VALUE = "value";
        public static final String TYPE = "type";
        public static final String TYPE_LIST = "list";
        public static final String TYPE_STRING = "string";
        public static final String TYPE_DOUBLE = "double";
        public static final String TYPE_INTEGER = "integer";
        public static final String TYPE_FLOAT = "float";
        public static final String TYPE_MAP = "map";
        public static final String TYPE_BOOLEAN = "boolean";
        public static final String TYPE_LONG = "long";
        public static final String TYPE_SHORT = "short";
        public static final String TYPE_BYTE = "byte";
        public static final String TYPE_UNKNOWN = "unknown";

        public static final String VERTICES = "vertices";
        public static final String EDGES = "edges";
        public static final String MODE = "mode";
    }
}
