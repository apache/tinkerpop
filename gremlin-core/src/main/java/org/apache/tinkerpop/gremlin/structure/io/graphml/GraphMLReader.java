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
package org.apache.tinkerpop.gremlin.structure.io.graphml;

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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * {@code GraphMLReader} writes the data from a GraphML stream to a graph.  Note that this format is lossy, in the
 * sense that data types and features of Gremlin Structure not supported by GraphML are not serialized.  This format
 * is meant for external export of a graph to tools outside of Gremlin Structure graphs.  Note that GraphML does not
 * support the notion of multi-properties or properties on properties.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Alex Averbuch (alex.averbuch@gmail.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphMLReader implements GraphReader {

    private final String edgeLabelKey;
    private final String vertexLabelKey;
    private final long batchSize;
    private final boolean strict;
    private final XMLInputFactory inputFactory;

    private GraphMLReader(final Builder builder) {
        this.edgeLabelKey = builder.edgeLabelKey;
        this.batchSize = builder.batchSize;
        this.vertexLabelKey = builder.vertexLabelKey;
        this.strict = builder.strict;
        this.inputFactory = builder.inputFactory;
    }

    @Override
    public void readGraph(final InputStream graphInputStream, final Graph graphToWriteTo) throws IOException {
        final Map<Object, Vertex> cache = new HashMap<>();
        final AtomicLong counter = new AtomicLong(0);
        final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();
        final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
        final Graph.Features.VertexFeatures vertexFeatures = graphToWriteTo.features().vertex();

        try {
            final XMLStreamReader reader = inputFactory.createXMLStreamReader(graphInputStream);
            final Map<String, String> keyIdMap = new HashMap<>();
            final Map<String, String> keyTypesMaps = new HashMap<>();

            // Buffered Data
            String dataId = null;
            final Map<String, String> defaultValues = new HashMap<>();

            // Buffered Vertex Data
            String vertexId = null;
            String vertexLabel = null;
            Map<String, Object> vertexProps = null;
            boolean isInVertex = false;

            // Buffered Edge Data
            String edgeId = null;
            String edgeLabel = null;
            Vertex edgeInVertex = null;
            Vertex edgeOutVertex = null;
            Map<String, Object> edgeProps = null;
            boolean isInEdge = false;

            while (reader.hasNext()) {
                final Integer eventType = reader.next();
                if (eventType.equals(XMLEvent.START_ELEMENT)) {
                    final String elementName = reader.getName().getLocalPart();

                    switch (elementName) {
                        case GraphMLTokens.KEY:
                            final String id = reader.getAttributeValue(null, GraphMLTokens.ID);
                            final String attributeName = reader.getAttributeValue(null, GraphMLTokens.ATTR_NAME);
                            final String attributeType = reader.getAttributeValue(null, GraphMLTokens.ATTR_TYPE);
                            keyIdMap.put(id, attributeName);
                            keyTypesMaps.put(id, attributeType);
                            dataId = id;
                            break;
                        case GraphMLTokens.NODE:
                            vertexId = reader.getAttributeValue(null, GraphMLTokens.ID);
                            isInVertex = true;
                            vertexProps = new HashMap<>();
                            break;
                        case GraphMLTokens.EDGE:
                            edgeId = reader.getAttributeValue(null, GraphMLTokens.ID);

                            final String vertexIdOut = reader.getAttributeValue(null, GraphMLTokens.SOURCE);
                            final String vertexIdIn = reader.getAttributeValue(null, GraphMLTokens.TARGET);

                            // graphml allows edges and vertices to be mixed in terms of how they are positioned
                            // in the xml therefore it is possible that an edge is created prior to its definition
                            // as a vertex.
                            edgeOutVertex = findOrCreate(vertexIdOut, graphToWriteTo, vertexFeatures, cache, false);
                            edgeInVertex = findOrCreate(vertexIdIn, graphToWriteTo, vertexFeatures, cache, false);

                            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                                graphToWriteTo.tx().commit();

                            isInEdge = true;
                            edgeProps = new HashMap<>();

                            break;
                        case GraphMLTokens.DATA:
                            final String key = reader.getAttributeValue(null, GraphMLTokens.KEY);
                            final String dataAttributeName = keyIdMap.get(key);
                            final String defaultValue = defaultValues.get(key);

                            if (dataAttributeName != null) {
                                String elementValue =  reader.getElementText();
                                final String value = elementValue.length() == 0 && defaultValue != null && defaultValue.length() != 0 ? defaultValue : elementValue;

                                if (isInVertex) {
                                    if (key.equals(vertexLabelKey))
                                        vertexLabel = value;
                                    else {
                                        try {
                                            vertexProps.put(dataAttributeName, typeCastValue(key, value, keyTypesMaps));
                                        } catch (NumberFormatException nfe) {
                                            if (strict) throw nfe;
                                        }
                                    }
                                } else if (isInEdge) {
                                    if (key.equals(edgeLabelKey))
                                        edgeLabel = value;
                                    else {
                                        try {
                                            edgeProps.put(dataAttributeName, typeCastValue(key, value, keyTypesMaps));
                                        } catch (NumberFormatException nfe) {
                                            if (strict) throw nfe;
                                        }
                                    }
                                }
                            }

                            break;

                        case GraphMLTokens.DEFAULT:
                            //TODO will it throw exception for CDATA etc?
                            defaultValues.putIfAbsent(dataId, reader.getElementText());
                            break;
                    }
                } else if (eventType.equals(XMLEvent.END_ELEMENT)) {
                    final String elementName = reader.getName().getLocalPart();
                    if(elementName.equals(GraphMLTokens.KEY)) {
                        dataId = null;
                    }
                    if (elementName.equals(GraphMLTokens.NODE)) {
                        final String currentVertexId = vertexId;
                        final String currentVertexLabel = Optional.ofNullable(vertexLabel).orElse(Vertex.DEFAULT_LABEL);
                        final Object[] propsAsArray = vertexProps.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue())).toArray();

                        findOrCreate(currentVertexId, graphToWriteTo, vertexFeatures, cache,
                                true, ElementHelper.upsert(propsAsArray, T.label, currentVertexLabel));

                        if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                            graphToWriteTo.tx().commit();

                        vertexId = null;
                        vertexLabel = null;
                        vertexProps = null;
                        isInVertex = false;
                    } else if (elementName.equals(GraphMLTokens.EDGE)) {
                        final Object[] propsAsArray = edgeProps.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue())).toArray();
                        final Object[] propsReady = null != edgeId && edgeFeatures.willAllowId(edgeId) ? ElementHelper.upsert(propsAsArray, T.id, edgeId) : propsAsArray;

                        edgeOutVertex.addEdge(null == edgeLabel ? Edge.DEFAULT_LABEL : edgeLabel, edgeInVertex, propsReady);

                        if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                            graphToWriteTo.tx().commit();

                        edgeId = null;
                        edgeLabel = null;
                        edgeOutVertex = null;
                        edgeInVertex = null;
                        edgeProps = null;
                        isInEdge = false;
                    }

                }
            }

            if (supportsTx) graphToWriteTo.tx().commit();
        } catch (XMLStreamException xse) {
            // rollback whatever portion failed
            if (supportsTx && counter.incrementAndGet() % batchSize == 0)
                graphToWriteTo.tx().rollback();
            throw new IOException(xse);
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
    public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod) throws IOException {
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

    private static Vertex findOrCreate(final Object id, final Graph graphToWriteTo,
                                       final Graph.Features.VertexFeatures features,
                                       final Map<Object, Vertex> cache, final boolean asVertex, final Object... args) {
        if (cache.containsKey(id)) {
            // if the request to findOrCreate come from a vertex then AND the vertex was already created, that means
            // that the vertex was created by an edge that arrived first in the stream (allowable via GraphML
            // specification).  as the edge only carries the vertex id and not its properties, the properties
            // of the vertex need to be attached at this point.
            if (asVertex) {
                final Vertex v = cache.get(id);
                ElementHelper.attachProperties(v, args);
                return v;
            } else {
                return cache.get(id);
            }
        } else {
            final Object[] argsReady = features.willAllowId(id) ? ElementHelper.upsert(args, T.id, id) : args;
            final Vertex v = graphToWriteTo.addVertex(argsReady);
            cache.put(id, v);
            return v;
        }
    }

    private static Object typeCastValue(final String key, final String value, final Map<String, String> keyTypes) {
        final String type = keyTypes.get(key);
        if (null == type || type.equals(GraphMLTokens.STRING))
            return value;
        else if (type.equals(GraphMLTokens.FLOAT))
            return Float.valueOf(value);
        else if (type.equals(GraphMLTokens.INT))
            return Integer.valueOf(value);
        else if (type.equals(GraphMLTokens.DOUBLE))
            return Double.valueOf(value);
        else if (type.equals(GraphMLTokens.BOOLEAN))
            return Boolean.valueOf(value);
        else if (type.equals(GraphMLTokens.LONG))
            return Long.valueOf(value);
        else
            return value;
    }

    public static Builder build() {
        return new Builder();
    }

    /**
     * Allows configuration and construction of the GraphMLReader instance.
     */
    public static final class Builder implements ReaderBuilder<GraphMLReader> {
        private String edgeLabelKey = GraphMLTokens.LABEL_E;
        private String vertexLabelKey = GraphMLTokens.LABEL_V;
        private boolean strict = true;
        private long batchSize = 10000;
        private XMLInputFactory inputFactory;

        private Builder() {
        }

        /**
         * When set to true, exceptions will be thrown if a property value cannot be coerced to the expected data
         * type. If set to false, then the reader will continue with the import but ignore the failed property key.
         * By default this value is "true".
         */
        public Builder strict(final boolean strict) {
            this.strict = strict;
            return this;
        }

        /**
         * The key to use as the edge label.
         */
        public Builder edgeLabelKey(final String edgeLabelKey) {
            this.edgeLabelKey = edgeLabelKey;
            return this;
        }

        /**
         * the key to use as the vertex label.
         */
        public Builder vertexLabelKey(final String vertexLabelKey) {
            this.vertexLabelKey = vertexLabelKey;
            return this;
        }

        /**
         * Number of mutations to perform before a commit is executed.
         */
        public Builder batchSize(final long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * A custom {@code XMLInputFactory}. If this value is not set then a default one is constructed. The default
         * will be configured to disable DTDs and support of external entities to prevent
         * <a href="https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html#xmlinputfactory-a-stax-parser">XXE</a>
         * style attacks.
         */
        public Builder xmlInputFactory(final XMLInputFactory inputFactory) {
            this.inputFactory = inputFactory;
            return this;
        }

        public GraphMLReader create() {
            if (this.inputFactory == null) {
                this.inputFactory = XMLInputFactory.newInstance();

                // prevent XXE
                // https://owasp.org/www-community/vulnerabilities/XML_External_Entity_(XXE)_Processing
                inputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
                inputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            }
            return new GraphMLReader(this);
        }
    }
}
