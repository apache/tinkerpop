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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * GraphMLWriter writes a Graph to a GraphML OutputStream. Note that this format is lossy, in the sense that data
 * types and features of Gremlin Structure not supported by GraphML are not serialized.  This format is meant for
 * external export of a graph to tools outside of Gremlin Structure graphs.  Note that GraphML does not support
 * the notion of multi-properties or properties on properties and will throw an exception when writing a
 * graph elements that have such things.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphMLWriter implements GraphWriter {
    private final XMLOutputFactory inputFactory = XMLOutputFactory.newInstance();
    private boolean normalize = false;

    private final Optional<Map<String, String>> vertexKeyTypes;
    private final Optional<Map<String, String>> edgeKeyTypes;
    private final Optional<String> xmlSchemaLocation;
    private final String edgeLabelKey;
    private final String vertexLabelKey;
    private Collection<String> intersection = Collections.emptySet();

    private GraphMLWriter(final boolean normalize, final Map<String, String> vertexKeyTypes,
            final Map<String, String> edgeKeyTypes, final String xmlSchemaLocation,
            final String edgeLabelKey, final String vertexLabelKey) {
        this.normalize = normalize;
        this.vertexKeyTypes = Optional.ofNullable(vertexKeyTypes);
        this.edgeKeyTypes = Optional.ofNullable(edgeKeyTypes);
        this.xmlSchemaLocation = Optional.ofNullable(xmlSchemaLocation);
        this.edgeLabelKey = edgeLabelKey;
        this.vertexLabelKey = vertexLabelKey;
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, Direction direction) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator, final Direction direction) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeVertexProperty(final OutputStream outputStream, final VertexProperty vp) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeProperty(final OutputStream outputStream, final Property p) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * This method is not supported for this writer.
     *
     * @throws UnsupportedOperationException when called.
     */
    @Override
    public void writeObject(final OutputStream outputStream, final Object object) throws IOException {
        throw Io.Exceptions.writerFormatIsForFullGraphSerializationOnly(this.getClass());
    }

    /**
     * Write the data in a Graph to a GraphML OutputStream.
     *
     * @param outputStream the GraphML OutputStream to write the Graph data to
     * @throws java.io.IOException thrown if there is an error generating the GraphML data
     */
    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        final Map<String, String> identifiedVertexKeyTypes = this.vertexKeyTypes.orElseGet(() -> GraphMLWriter.determineVertexTypes(g));
        final Map<String, String> identifiedEdgeKeyTypes = this.edgeKeyTypes.orElseGet(() -> GraphMLWriter.determineEdgeTypes(g));

        if (identifiedEdgeKeyTypes.containsKey(this.edgeLabelKey))
            throw new IllegalStateException(String.format("The edgeLabelKey value of[%s] conflicts with the name of an existing property key to be included in the GraphML", this.edgeLabelKey));
        if (identifiedEdgeKeyTypes.containsKey(this.edgeLabelKey))
            throw new IllegalStateException(String.format("The vertexLabelKey value of[%s] conflicts with the name of an existing property key to be included in the GraphML", this.vertexLabelKey));

        identifiedEdgeKeyTypes.put(this.edgeLabelKey, GraphMLTokens.STRING);
        identifiedVertexKeyTypes.put(this.vertexLabelKey, GraphMLTokens.STRING);

        try {
            final XMLStreamWriter writer;
            writer = configureWriter(outputStream);

            writer.writeStartDocument();
            writer.writeStartElement(GraphMLTokens.GRAPHML);
            writeXmlNsAndSchema(writer);

            writeTypes(identifiedVertexKeyTypes, identifiedEdgeKeyTypes, writer);

            writer.writeStartElement(GraphMLTokens.GRAPH);
            writer.writeAttribute(GraphMLTokens.ID, GraphMLTokens.G);
            writer.writeAttribute(GraphMLTokens.EDGEDEFAULT, GraphMLTokens.DIRECTED);

            writeVertices(writer, g);
            writeEdges(writer, g);

            writer.writeEndElement(); // graph
            writer.writeEndElement(); // graphml
            writer.writeEndDocument();

            writer.flush();
            writer.close();
        } catch (XMLStreamException xse) {
            throw new IOException(xse);
        }
    }

    private XMLStreamWriter configureWriter(final OutputStream outputStream) throws XMLStreamException {
        final XMLStreamWriter utf8Writer = inputFactory.createXMLStreamWriter(outputStream, "UTF8");
        if (normalize) {
            final XMLStreamWriter writer = new GraphMLWriterHelper.IndentingXMLStreamWriter(utf8Writer);
            ((GraphMLWriterHelper.IndentingXMLStreamWriter) writer).setIndentStep("    ");
            return writer;
        } else
            return utf8Writer;
        }

    private void writeTypes(final Map<String, String> identifiedVertexKeyTypes,
            final Map<String, String> identifiedEdgeKeyTypes,
            final XMLStreamWriter writer) throws XMLStreamException {
        // <key id="weight" for="edge" attr.name="weight" attr.type="float"/>
        final Collection<String> vertexKeySet = getVertexKeysAndNormalizeIfRequired(identifiedVertexKeyTypes);
        final Collection<String> edgeKeySet = getEdgeKeysAndNormalizeIfRequired(identifiedEdgeKeyTypes);
        // in case vertex and edge may have the same attribute name, the key id in graphml have to be different
        intersection = CollectionUtils.intersection(vertexKeySet, edgeKeySet);

        for (String key : vertexKeySet) {
            writer.writeStartElement(GraphMLTokens.KEY);
            if (intersection.contains(key)) {
                writer.writeAttribute(GraphMLTokens.ID, key.concat(GraphMLTokens.VERTEX_SUFFIX));
            } else {
                writer.writeAttribute(GraphMLTokens.ID, key);
            }
            writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.NODE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, identifiedVertexKeyTypes.get(key));
            writer.writeEndElement();
        }
        for (String key : edgeKeySet) {
            writer.writeStartElement(GraphMLTokens.KEY);
            if (intersection.contains(key)) {
                writer.writeAttribute(GraphMLTokens.ID, key.concat(GraphMLTokens.EDGE_SUFFIX));
            } else {
                writer.writeAttribute(GraphMLTokens.ID, key);
            }
            writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.EDGE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, identifiedEdgeKeyTypes.get(key));
            writer.writeEndElement();
        }
    }

    private void writeEdges(final XMLStreamWriter writer, final Graph graph) throws XMLStreamException {
        if (normalize) {
            final List<Edge> edges = IteratorUtils.list(graph.edges());
            Collections.sort(edges, Comparators.ELEMENT_COMPARATOR);

            for (Edge edge : edges) {
                writer.writeStartElement(GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ID, edge.id().toString());
                writer.writeAttribute(GraphMLTokens.SOURCE, edge.outVertex().id().toString());
                writer.writeAttribute(GraphMLTokens.TARGET, edge.inVertex().id().toString());

                writer.writeStartElement(GraphMLTokens.DATA);
                writer.writeAttribute(GraphMLTokens.KEY, this.edgeLabelKey);
                writer.writeCharacters(edge.label());
                writer.writeEndElement();

                final List<String> keys = new ArrayList<>(edge.keys());
                Collections.sort(keys);

                for (String key : keys) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    if (intersection != null && intersection.contains(key)) {
                        writer.writeAttribute(GraphMLTokens.KEY, key + GraphMLTokens.EDGE_SUFFIX);
                    } else {
                        writer.writeAttribute(GraphMLTokens.KEY, key);
                    }
                    // technically there can't be a null here as gremlin structure forbids that occurrence even if Graph
                    // implementations support it, but out to empty string just in case.
                    writer.writeCharacters(edge.property(key).orElse("").toString());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }
        } else {
            final Iterator<Edge> iterator = graph.edges();
            while (iterator.hasNext()) {
                final Edge edge = iterator.next();
                writer.writeStartElement(GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ID, edge.id().toString());
                writer.writeAttribute(GraphMLTokens.SOURCE, edge.outVertex().id().toString());
                writer.writeAttribute(GraphMLTokens.TARGET, edge.inVertex().id().toString());

                writer.writeStartElement(GraphMLTokens.DATA);
                writer.writeAttribute(GraphMLTokens.KEY, this.edgeLabelKey);
                writer.writeCharacters(edge.label());
                writer.writeEndElement();

                for (String key : edge.keys()) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    if (intersection != null && intersection.contains(key)) {
                        writer.writeAttribute(GraphMLTokens.KEY, key + GraphMLTokens.EDGE_SUFFIX);
                    } else {
                        writer.writeAttribute(GraphMLTokens.KEY, key);
                    }
                    // technically there can't be a null here as gremlin structure forbids that occurrence even if Graph
                    // implementations support it, but out to empty string just in case.
                    writer.writeCharacters(edge.property(key).orElse("").toString());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }
        }
    }

    private void writeVertices(final XMLStreamWriter writer, final Graph graph) throws XMLStreamException {
        final Iterable<Vertex> vertices = getVerticesAndNormalizeIfRequired(graph);
        for (Vertex vertex : vertices) {
            writer.writeStartElement(GraphMLTokens.NODE);
            writer.writeAttribute(GraphMLTokens.ID, vertex.id().toString());
            final Collection<String> keys = getElementKeysAndNormalizeIfRequired(vertex);

            writer.writeStartElement(GraphMLTokens.DATA);
            writer.writeAttribute(GraphMLTokens.KEY, this.vertexLabelKey);
            writer.writeCharacters(vertex.label());
            writer.writeEndElement();

            for (String key : keys) {
                writer.writeStartElement(GraphMLTokens.DATA);
                if (intersection != null && intersection.contains(key)) {
                    writer.writeAttribute(GraphMLTokens.KEY, key.concat(GraphMLTokens.VERTEX_SUFFIX));
                } else {
                    writer.writeAttribute(GraphMLTokens.KEY, key);
                }

                final VertexProperty<Object> currentValue = getCheckedVertexProperty(vertex, key);

                // technically there can't be a null here as gremlin structure forbids that occurrence even if Graph
                // implementations support it, but out to empty string just in case.
                writer.writeCharacters(currentValue.orElse("").toString());
                writer.writeEndElement();
            }
            writer.writeEndElement();
        }
    }

    private Collection<String> getElementKeysAndNormalizeIfRequired(final Element element) {
        final Collection<String> keys;
        if (normalize) {
            keys = new ArrayList<>();
            keys.addAll(element.keys());
            Collections.sort((List<String>) keys);
        } else
            keys = element.keys();

        return keys;
    }

    private Iterable<Vertex> getVerticesAndNormalizeIfRequired(final Graph graph) {
        final Iterable<Vertex> vertices;
        if (normalize) {
            vertices = new ArrayList<>();
            final Iterator<Vertex> vertexIterator = graph.vertices();
            while (vertexIterator.hasNext()) {
                ((Collection<Vertex>) vertices).add(vertexIterator.next());
            }
            Collections.sort((List<Vertex>) vertices, Comparators.ELEMENT_COMPARATOR);
        } else
            vertices = IteratorUtils.list(graph.vertices());

        return vertices;
    }

    private Collection<String> getEdgeKeysAndNormalizeIfRequired(final Map<String, String> identifiedEdgeKeyTypes) {
        final Collection<String> edgeKeySet;
        if (normalize) {
            edgeKeySet = new ArrayList<>();
            edgeKeySet.addAll(identifiedEdgeKeyTypes.keySet());
            Collections.sort((List<String>) edgeKeySet);
        } else
            edgeKeySet = identifiedEdgeKeyTypes.keySet();

        return edgeKeySet;
    }

    private Collection<String> getVertexKeysAndNormalizeIfRequired(final Map<String, String> identifiedVertexKeyTypes) {
        final Collection<String> keyset;
        if (normalize) {
            keyset = new ArrayList<>();
            keyset.addAll(identifiedVertexKeyTypes.keySet());
            Collections.sort((List<String>) keyset);
        } else
            keyset = identifiedVertexKeyTypes.keySet();

        return keyset;
    }

    private void writeXmlNsAndSchema(final XMLStreamWriter writer) throws XMLStreamException {
        writer.writeAttribute(GraphMLTokens.XMLNS, GraphMLTokens.GRAPHML_XMLNS);

        //XML Schema instance namespace definition (xsi)
        writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ':' + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
                XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
        //XML Schema location
        writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ':' + GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE,
                GraphMLTokens.GRAPHML_XMLNS + ' ' + this.xmlSchemaLocation.orElse(GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION));
    }

    private static Map<String, String> determineVertexTypes(final Graph graph) {
        final Map<String, String> vertexKeyTypes = new HashMap<>();
        final Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            final Vertex vertex = vertices.next();
            for (String key : vertex.keys()) {
                if (!vertexKeyTypes.containsKey(key)) {
                    final VertexProperty<Object> currentValue = getCheckedVertexProperty(vertex, key);

                    vertexKeyTypes.put(key, GraphMLWriter.getStringType(currentValue.value()));
                }
            }
        }

        return vertexKeyTypes;
    }

    private static VertexProperty<Object> getCheckedVertexProperty(Vertex vertex, String key) {
        final Iterator<VertexProperty<Object>> properties = vertex.properties(key);
        final VertexProperty<Object> currentValue = properties.next();

        if (properties.hasNext())
            throw new IllegalStateException("Multiple properties exists for the provided key: [%s] and multi-properties are not directly supported by GraphML format");
        return currentValue;
    }

    private static Map<String, String> determineEdgeTypes(final Graph graph) {
        final Map<String, String> edgeKeyTypes = new HashMap<>();
        final Iterator<Edge> edges = graph.edges();
        while (edges.hasNext()) {
            final Edge edge = edges.next();
            for (String key : edge.keys()) {
                if (!edgeKeyTypes.containsKey(key))
                    edgeKeyTypes.put(key, GraphMLWriter.getStringType(edge.property(key).value()));
                }
            }

        return edgeKeyTypes;
    }

    private static String getStringType(final Object object) {
        if (object instanceof String)
            return GraphMLTokens.STRING;
        else if (object instanceof Integer)
            return GraphMLTokens.INT;
        else if (object instanceof Long)
            return GraphMLTokens.LONG;
        else if (object instanceof Float)
            return GraphMLTokens.FLOAT;
        else if (object instanceof Double)
            return GraphMLTokens.DOUBLE;
        else if (object instanceof Boolean)
            return GraphMLTokens.BOOLEAN;
        else
            return GraphMLTokens.STRING;
        }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder implements WriterBuilder<GraphMLWriter> {
        private boolean normalize = false;
        private Map<String, String> vertexKeyTypes = null;
        private Map<String, String> edgeKeyTypes = null;

        private String xmlSchemaLocation = null;
        private String edgeLabelKey = GraphMLTokens.LABEL_E;
        private String vertexLabelKey = GraphMLTokens.LABEL_V;

        private Builder() {
        }

        /**
         * Normalized output is deterministic with respect to the order of elements and properties in the resulting
         * XML document, and is compatible with line diff-based tools such as Git. Note: normalized output is
         * sideEffects-intensive and is not appropriate for very large graphs.
         *
         * @param normalize whether to normalize the output.
         */
        public Builder normalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        /**
         * Map of the data types of the vertex keys.  It is best to specify this Map if possible as the only
         * other way to attain it is to iterate all vertices to find all the property keys.
         */
        public Builder vertexKeyTypes(final Map<String, String> vertexKeyTypes) {
            this.vertexKeyTypes = vertexKeyTypes;
            return this;
        }

        /**
         * Map of the data types of the edge keys.  It is best to specify this Map if possible as the only
         * other way to attain it is to iterate all vertices to find all the property keys.
         */
        public Builder edgeKeyTypes(final Map<String, String> edgeKeyTypes) {
            this.edgeKeyTypes = edgeKeyTypes;
            return this;
        }

        /**
         * Location of the GraphML schema which is defaulted to
         * {@link org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLTokens#DEFAULT_GRAPHML_SCHEMA_LOCATION}.
         */
        public Builder xmlSchemaLocation(final String xmlSchemaLocation) {
            this.xmlSchemaLocation = xmlSchemaLocation;
            return this;
        }

        /**
         * Set the name of the edge label in the GraphML. This value is defaulted to {@link GraphMLTokens#LABEL_E}.
         * The value of {@link org.apache.tinkerpop.gremlin.structure.Edge#label()} is written as a data element on the edge
         * and the appropriate key element is added to define it in the GraphML.  It is important that when reading
         * this GraphML back in with the reader that this label key is set appropriately to properly read the edge
         * labels.
         *
         * @param edgeLabelKey if the label of an edge will be handled by the data property.
         */
        public Builder edgeLabelKey(final String edgeLabelKey) {
            this.edgeLabelKey = edgeLabelKey;
            return this;
        }

        /**
         * Set the name of the vertex label in the GraphML. This value is defaulted to {@link GraphMLTokens#LABEL_V}.
         * The value of {@link org.apache.tinkerpop.gremlin.structure.Vertex#label()} is written as a data element on the
         * vertex and the appropriate key element is added to define it in the GraphML.  It is important that when
         * reading this GraphML back in with the reader that this label key is set appropriately to properly read the
         * vertex labels.
         *
         * @param vertexLabelKey if the label of an vertex will be handled by the data property.
         */
        public Builder vertexLabelKey(final String vertexLabelKey) {
            this.vertexLabelKey = vertexLabelKey;
            return this;
        }

        public GraphMLWriter create() {
            return new GraphMLWriter(normalize, vertexKeyTypes, edgeKeyTypes, xmlSchemaLocation, edgeLabelKey, vertexLabelKey);
        }
    }
}

