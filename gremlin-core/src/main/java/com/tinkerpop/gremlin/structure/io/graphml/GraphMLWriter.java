package com.tinkerpop.gremlin.structure.io.graphml;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * GraphMLWriter writes a Graph to a GraphML OutputStream. Note that this format is lossy, in the sense that data
 * types and features of Gremlin Structure not supported by GraphML are not serialized.  This format is meant for
 * external export of a graph to tools outside of Gremlin Structure graphs.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMLWriter implements GraphWriter {
    private static final Comparator<Element> ELEMENT_COMPARATOR = Comparator.comparing(e -> e.getId().toString(), String.CASE_INSENSITIVE_ORDER);
    private final XMLOutputFactory inputFactory = XMLOutputFactory.newInstance();
    private final Graph graph;
    private boolean normalize = false;

    private final Optional<Map<String, String>> vertexKeyTypes;
    private final Optional<Map<String, String>> edgeKeyTypes;
    private final Optional<String> xmlSchemaLocation;
    private final Optional<String> edgeLabelKey;

    private GraphMLWriter(final Graph graph, final boolean normalize, final Map<String, String> vertexKeyTypes,
                          final Map<String, String> edgeKeyTypes, final String xmlSchemaLocation,
                          final String edgeLabelKey) {
        this.graph = graph;
        this.normalize = normalize;
        this.vertexKeyTypes = Optional.ofNullable(vertexKeyTypes);
        this.edgeKeyTypes = Optional.ofNullable(edgeKeyTypes);
        this.xmlSchemaLocation = Optional.ofNullable(xmlSchemaLocation);
        this.edgeLabelKey = Optional.ofNullable(edgeLabelKey);
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, Direction direction) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    /**
     * Write the data in a Graph to a GraphML OutputStream.
     *
     * @param outputStream the GraphML OutputStream to write the Graph data to
     * @throws java.io.IOException thrown if there is an error generating the GraphML data
     */
    @Override
    public void writeGraph(final OutputStream outputStream) throws IOException {
        final Map<String, String> identifiedVertexKeyTypes = this.vertexKeyTypes.orElseGet(this::determineVertexTypes);
        final Map<String, String> identifiedEdgeKeyTypes = this.edgeKeyTypes.orElseGet(this::determineEdgeTypes);

        // adding the edge label key will push the label into the data portion of the graphml otherwise it
        // will live with the edge data itself (which won't validate against the graphml schema)
        if (this.edgeLabelKey.isPresent() && null == identifiedEdgeKeyTypes.get(this.edgeLabelKey.get()))
            identifiedEdgeKeyTypes.put(this.edgeLabelKey.get(), GraphMLTokens.STRING);

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

            writeVertices(writer);
            writeEdges(writer);

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
        for (String key : vertexKeySet) {
            writer.writeStartElement(GraphMLTokens.KEY);
            writer.writeAttribute(GraphMLTokens.ID, key);
            writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.NODE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, identifiedVertexKeyTypes.get(key));
            writer.writeEndElement();
        }

        final Collection<String> edgeKeySet = getEdgeKeysAndNormalizeIfRequired(identifiedEdgeKeyTypes);
        for (String key : edgeKeySet) {
            writer.writeStartElement(GraphMLTokens.KEY);
            writer.writeAttribute(GraphMLTokens.ID, key);
            writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.EDGE);
            writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
            writer.writeAttribute(GraphMLTokens.ATTR_TYPE, identifiedEdgeKeyTypes.get(key));
            writer.writeEndElement();
        }
    }

    private void writeEdges(final XMLStreamWriter writer) throws XMLStreamException {
        if (normalize) {
            final List<Edge> edges = graph.E().toList();
            Collections.sort(edges, ELEMENT_COMPARATOR);

            for (Edge edge : edges) {
                writer.writeStartElement(GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ID, edge.getId().toString());
                writer.writeAttribute(GraphMLTokens.SOURCE, edge.getVertex(Direction.OUT).getId().toString());
                writer.writeAttribute(GraphMLTokens.TARGET, edge.getVertex(Direction.IN).getId().toString());

                if (this.edgeLabelKey.isPresent()) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    writer.writeAttribute(GraphMLTokens.KEY, this.edgeLabelKey.get());
                    writer.writeCharacters(edge.getLabel());
                    writer.writeEndElement();
                } else {
                    // this will not comply with the graphml schema but is here so that the label is not
                    // mixed up with properties.
                    writer.writeAttribute(GraphMLTokens.LABEL, edge.getLabel());
                }

                final List<String> keys = new ArrayList<>();
                keys.addAll(edge.getPropertyKeys());
                Collections.sort(keys);

                for (String key : keys) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    writer.writeAttribute(GraphMLTokens.KEY, key);
                    // technically there can't be a null here as Blueprints forbids that occurrence even if Graph
                    // implementations support it, but out to empty string just in case.
                    writer.writeCharacters(edge.getProperty(key).orElse("").toString());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }
        } else {
            for (Edge edge : graph.E().toList()) {
                writer.writeStartElement(GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ID, edge.getId().toString());
                writer.writeAttribute(GraphMLTokens.SOURCE, edge.getVertex(Direction.OUT).getId().toString());
                writer.writeAttribute(GraphMLTokens.TARGET, edge.getVertex(Direction.IN).getId().toString());
                writer.writeAttribute(GraphMLTokens.LABEL, edge.getLabel());

                for (String key : edge.getPropertyKeys()) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    writer.writeAttribute(GraphMLTokens.KEY, key);
                    // technically there can't be a null here as Blueprints forbids that occurrence even if Graph
                    // implementations support it, but out to empty string just in case.
                    writer.writeCharacters(edge.getProperty(key).orElse("").toString());
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }
        }
    }

    private void writeVertices(final XMLStreamWriter writer) throws XMLStreamException {
        final Iterable<Vertex> vertices = getVerticesAndNormalizeIfRequired();
        for (Vertex vertex : vertices) {
            writer.writeStartElement(GraphMLTokens.NODE);
            writer.writeAttribute(GraphMLTokens.ID, vertex.getId().toString());
            final Collection<String> keys = getElementKeysAndNormalizeIfRequired(vertex);
            for (String key : keys) {
                writer.writeStartElement(GraphMLTokens.DATA);
                writer.writeAttribute(GraphMLTokens.KEY, key);
                // technically there can't be a null here as Blueprints forbids that occurrence even if Graph
                // implementations support it, but out to empty string just in case.
                writer.writeCharacters(vertex.getProperty(key).orElse("").toString());
                writer.writeEndElement();
            }
            writer.writeEndElement();
        }
    }

    private Collection<String> getElementKeysAndNormalizeIfRequired(final Element element) {
        final Collection<String> keys;
        if (normalize) {
            keys = new ArrayList<>();
            keys.addAll(element.getPropertyKeys());
            Collections.sort((List<String>) keys);
        } else
            keys = element.getPropertyKeys();

        return keys;
    }

    private Iterable<Vertex> getVerticesAndNormalizeIfRequired() {
        final Iterable<Vertex> vertices;
        if (normalize) {
            vertices = new ArrayList<>();
            for (Vertex v : graph.V().toList()) {
                ((Collection<Vertex>) vertices).add(v);
            }
            Collections.sort((List<Vertex>) vertices, ELEMENT_COMPARATOR);
        } else
            vertices = graph.V().toList();

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
        writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ":" + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
                XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
        //XML Schema location
        writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ":" + GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE,
                GraphMLTokens.GRAPHML_XMLNS + " " + this.xmlSchemaLocation.orElse(GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION));
    }

    private Map<String, String> determineVertexTypes() {
        final Map<String, String> vertexKeyTypes = new HashMap<>();
        for (Vertex vertex : graph.V().toList()) {
            for (String key : vertex.getPropertyKeys()) {
                if (!vertexKeyTypes.containsKey(key)) {
                    vertexKeyTypes.put(key, GraphMLWriter.getStringType(vertex.getProperty(key).get()));
                }
            }
        }

        return vertexKeyTypes;
    }

    private Map<String, String> determineEdgeTypes() {
        final Map<String, String> edgeKeyTypes = new HashMap<>();
        for (Edge edge : graph.E().toList()) {
            for (String key : edge.getPropertyKeys()) {
                if (!edgeKeyTypes.containsKey(key))
                    edgeKeyTypes.put(key, GraphMLWriter.getStringType(edge.getProperty(key).get()));
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

    public static final class Builder {
        private final Graph g;
        private boolean normalize = false;
        private Map<String, String> vertexKeyTypes = null;
        private Map<String, String> edgeKeyTypes = null;

        private String xmlSchemaLocation = null;
        private String edgeLabelKey = null;

        /**
         * Constructs a GraphMLWriter.
         *
         * @param g The Graph instance to write out.
         */
        public Builder(final Graph g) {
            if (null == g)
                throw new IllegalArgumentException("Graph argument cannot be null");

            this.g = g;
        }

        /**
         * Normalized output is deterministic with respect to the order of elements and properties in the resulting
         * XML document, and is compatible with line diff-based tools such as Git. Note: normalized output is
         * memory-intensive and is not appropriate for very large graphs.
         *
         * @param normalize whether to normalize the output.
         */
        public Builder setNormalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        /**
         * Map of the data types of the vertex keys.
         */
        public Builder setVertexKeyTypes(final Map<String, String> vertexKeyTypes) {
            this.vertexKeyTypes = vertexKeyTypes;
            return this;
        }

        /**
         * Map of the data types of the edge keys.
         */
        public Builder setEdgeKeyTypes(final Map<String, String> edgeKeyTypes) {
            this.edgeKeyTypes = edgeKeyTypes;
            return this;
        }

        public Builder setXmlSchemaLocation(final String xmlSchemaLocation) {
            this.xmlSchemaLocation = xmlSchemaLocation;
            return this;
        }

        /**
         * Set the name of the edge label in the GraphML. When this value is not set the value of the Edge.getLabel()
         * is written as a "label" attribute on the edge element.  This does not validate against the GraphML schema.
         * If this value is set then the the value of Edge.getLabel() is written as a data element on the edge and
         * the appropriate key element is added to define it in the GraphML
         *
         * @param edgeLabelKey if the label of an edge will be handled by the data property.
         */
        public Builder setEdgeLabelKey(final String edgeLabelKey) {
            this.edgeLabelKey = edgeLabelKey;
            return this;
        }

        public GraphMLWriter build() {
            return new GraphMLWriter(g, normalize, vertexKeyTypes, edgeKeyTypes, xmlSchemaLocation, edgeLabelKey);
        }
    }
}

