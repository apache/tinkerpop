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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A @{link GraphReader} that constructs a graph from a JSON-based representation of a graph and its elements given
 * the "legacy" Blueprints 2.x version of GraphSON.  This implementation is specifically for aiding in migration
 * of graphs from TinkerPop 2.x to TinkerPop 3.x. This reader only reads GraphSON from TinkerPop 2.x that was
 * generated in {@code GraphSONMode.EXTENDED}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LegacyGraphSONReader implements GraphReader {
    private final ObjectMapper mapper;
    private final long batchSize;

    public LegacyGraphSONReader(final ObjectMapper mapper, final long batchSize) {
        this.mapper = mapper;
        this.batchSize = batchSize;
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        final BatchGraph<?> graph;
        try {
            // will throw an exception if not constructed properly
            graph = BatchGraph.build(graphToWriteTo)
                    .bufferSize(batchSize).create();
        } catch (Exception ex) {
            throw new IOException("Could not instantiate BatchGraph wrapper", ex);
        }

        final JsonFactory factory = mapper.getFactory();
        final GraphSONUtility graphson = new GraphSONUtility(graph);

        try (JsonParser parser = factory.createParser(inputStream)) {
            if (parser.nextToken() != JsonToken.START_OBJECT)
                throw new IOException("Expected data to start with an Object");

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String fieldName = parser.getCurrentName() == null ? "" : parser.getCurrentName();
                switch (fieldName) {
                    case GraphSONTokens.MODE:
                        parser.nextToken();
                        final String mode = parser.getText();
                        if (!mode.equals("EXTENDED"))
                            throw new IllegalStateException("The legacy GraphSON must be generated with GraphSONMode.EXTENDED");
                        break;
                    case GraphSONTokens.VERTICES:
                        parser.nextToken();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            final JsonNode node = parser.readValueAsTree();
                            graphson.vertexFromJson(node);
                        }
                        break;
                    case GraphSONTokens.EDGES:
                        parser.nextToken();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            final JsonNode node = parser.readValueAsTree();
                            final Vertex inV = graph.vertices(GraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokens._IN_V))).next();
                            final Vertex outV = graph.vertices(GraphSONUtility.getTypedValueFromJsonNode(node.get(GraphSONTokens._OUT_V))).next();
                            GraphSONUtility.edgeFromJson(node, outV, inV);
                        }
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unexpected token in GraphSON - %s", fieldName));
                }
            }

            graph.tx().commit();
        } catch (Exception ex) {
            throw new IOException(ex);
        }

    }

    @Override
    public Iterator<Vertex> readVertices(final InputStream inputStream, final Direction direction,
                                         final Function<DetachedVertex, Vertex> vertexMaker,
                                         final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException("This reader only reads an entire Graph");
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException("This reader only reads an entire Graph");
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<DetachedVertex, Vertex> vertexMaker) throws IOException {
        throw new UnsupportedOperationException("This reader only reads an entire Graph");
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction, final Function<DetachedVertex, Vertex> vertexMaker, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException("This reader only reads an entire Graph");
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private boolean loadCustomModules = false;
        private List<SimpleModule> customModules = new ArrayList<>();
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;
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

    public static class GraphSONUtility {
        private static final String EMPTY_STRING = "";
        private final Graph g;

        public GraphSONUtility(final Graph g) {
            this.g = g;
        }

        public Vertex vertexFromJson(final JsonNode json) throws IOException {
            final Map<String, Object> props = readProperties(json);

            final Object vertexId = getTypedValueFromJsonNode(json.get(GraphSONTokens._ID));
            final Vertex v = g.addVertex(T.id, vertexId);

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                v.property(VertexProperty.Cardinality.list, entry.getKey(), entry.getValue());
            }

            return v;
        }

        public static Edge edgeFromJson(final JsonNode json, final Vertex out, final Vertex in) throws IOException {
            final Map<String, Object> props = GraphSONUtility.readProperties(json);

            final Object edgeId = getTypedValueFromJsonNode(json.get(GraphSONTokens._ID));
            final JsonNode nodeLabel = json.get(GraphSONTokens._LABEL);
            final String label = nodeLabel == null ? EMPTY_STRING : nodeLabel.textValue();

            final Edge e = out.addEdge(label, in, T.id, edgeId);
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
            return key.equals(GraphSONTokens._ID) || key.equals(GraphSONTokens._TYPE) || key.equals(GraphSONTokens._LABEL)
                    || key.equals(GraphSONTokens._OUT_V) || key.equals(GraphSONTokens._IN_V);
        }

        private static Object readProperty(final JsonNode node) {
            final Object propertyValue;

            if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_UNKNOWN)) {
                propertyValue = null;
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_BOOLEAN)) {
                propertyValue = node.get(GraphSONTokens.VALUE).booleanValue();
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_FLOAT)) {
                propertyValue = Float.parseFloat(node.get(GraphSONTokens.VALUE).asText());
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_BYTE)) {
                propertyValue = Byte.parseByte(node.get(GraphSONTokens.VALUE).asText());
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_SHORT)) {
                propertyValue = Short.parseShort(node.get(GraphSONTokens.VALUE).asText());
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_DOUBLE)) {
                propertyValue = node.get(GraphSONTokens.VALUE).doubleValue();
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_INTEGER)) {
                propertyValue = node.get(GraphSONTokens.VALUE).intValue();
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_LONG)) {
                propertyValue = node.get(GraphSONTokens.VALUE).longValue();
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_STRING)) {
                propertyValue = node.get(GraphSONTokens.VALUE).textValue();
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_LIST)) {
                propertyValue = readProperties(node.get(GraphSONTokens.VALUE).elements());
            } else if (node.get(GraphSONTokens.TYPE).textValue().equals(GraphSONTokens.TYPE_MAP)) {
                propertyValue = readProperties(node.get(GraphSONTokens.VALUE));
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

    public static class GraphSONTokens {
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
