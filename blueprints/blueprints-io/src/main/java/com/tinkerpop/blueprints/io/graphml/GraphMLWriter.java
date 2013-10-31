package com.tinkerpop.blueprints.io.graphml;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.io.GraphWriter;
import com.tinkerpop.blueprints.io.LexicographicalElementComparator;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * GraphMLWriter writes a Graph to a GraphML OutputStream.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMLWriter implements GraphWriter {

    private final Graph graph;
    private boolean normalize = false;
    private Map<String, String> vertexKeyTypes = null;
    private Map<String, String> edgeKeyTypes = null;

    private String xmlSchemaLocation = null;
    private Optional<String> edgeLabelKey;

    /**
     * @param graph the Graph to pull the data from
     */
    private GraphMLWriter(final Graph graph, final boolean normalize, final Map<String, String> vertexKeyTypes,
                          final Map<String, String> edgeKeyTypes, final String xmlSchemaLocation,
                          final String edgeLabelKey) {
        this.graph = graph;
        this.normalize = normalize;
        this.vertexKeyTypes = vertexKeyTypes;
        this.edgeKeyTypes = edgeKeyTypes;
        this.xmlSchemaLocation = xmlSchemaLocation;
        this.edgeLabelKey = Optional.ofNullable(edgeLabelKey);
    }

    /**
     * Write the data in a Graph to a GraphML OutputStream.
     *
     * @param outputStream the GraphML OutputStream to write the Graph data to
     * @throws IOException thrown if there is an error generating the GraphML data
     */
    @Override
    public void outputGraph(final OutputStream outputStream) throws IOException {

        if (null == vertexKeyTypes || null == edgeKeyTypes) {
            Map<String, String> vertexKeyTypes = new HashMap<>();
            Map<String, String> edgeKeyTypes = new HashMap<>();

            for (Vertex vertex : graph.query().vertices()) {
                for (String key : vertex.getPropertyKeys()) {
                    if (!vertexKeyTypes.containsKey(key)) {
                        vertexKeyTypes.put(key, GraphMLWriter.getStringType(vertex.getProperty(key).getValue()));
                    }
                }
                for (Edge edge : vertex.query().direction(Direction.OUT).edges()) {
                    for (String key : edge.getPropertyKeys()) {
                        if (!edgeKeyTypes.containsKey(key)) {
                            edgeKeyTypes.put(key, GraphMLWriter.getStringType(edge.getProperty(key).getValue()));
                        }
                    }
                }
            }

            if (null == this.vertexKeyTypes) {
                this.vertexKeyTypes = vertexKeyTypes;
            }

            if (null == this.edgeKeyTypes) {
                this.edgeKeyTypes = edgeKeyTypes;
            }
        }

        // adding the edge label key will push the label into the data portion of the graphml otherwise it
        // will live with the edge data itself (which won't validate against the graphml schema)
        if (this.edgeLabelKey.isPresent() && null != this.edgeKeyTypes && null == this.edgeKeyTypes.get(this.edgeLabelKey.get()))
            this.edgeKeyTypes.put(this.edgeLabelKey.get(), GraphMLTokens.STRING);

        final XMLOutputFactory inputFactory = XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = inputFactory.createXMLStreamWriter(outputStream, "UTF8");
            if (normalize) {
                writer = new GraphMLWriterHelper.IndentingXMLStreamWriter(writer);
                ((GraphMLWriterHelper.IndentingXMLStreamWriter) writer).setIndentStep("    ");
            }

            writer.writeStartDocument();
            writer.writeStartElement(GraphMLTokens.GRAPHML);
            writer.writeAttribute(GraphMLTokens.XMLNS, GraphMLTokens.GRAPHML_XMLNS);

            //XML Schema instance namespace definition (xsi)
            writer.writeAttribute(XMLConstants.XMLNS_ATTRIBUTE + ":" + GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG,
                    XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI);
            //XML Schema location
            writer.writeAttribute(GraphMLTokens.XML_SCHEMA_NAMESPACE_TAG + ":" + GraphMLTokens.XML_SCHEMA_LOCATION_ATTRIBUTE,
                    GraphMLTokens.GRAPHML_XMLNS + " " + (this.xmlSchemaLocation == null ?
                            GraphMLTokens.DEFAULT_GRAPHML_SCHEMA_LOCATION : this.xmlSchemaLocation));

            // <key id="weight" for="edge" attr.name="weight" attr.type="float"/>
            Collection<String> keyset;

            if (normalize) {
                keyset = new ArrayList<>();
                keyset.addAll(vertexKeyTypes.keySet());
                Collections.sort((List<String>) keyset);
            } else {
                keyset = vertexKeyTypes.keySet();
            }
            for (String key : keyset) {
                writer.writeStartElement(GraphMLTokens.KEY);
                writer.writeAttribute(GraphMLTokens.ID, key);
                writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.NODE);
                writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
                writer.writeAttribute(GraphMLTokens.ATTR_TYPE, vertexKeyTypes.get(key));
                writer.writeEndElement();
            }

            if (normalize) {
                keyset = new ArrayList<>();
                keyset.addAll(edgeKeyTypes.keySet());
                Collections.sort((List<String>) keyset);
            } else {
                keyset = edgeKeyTypes.keySet();
            }
            for (String key : keyset) {
                writer.writeStartElement(GraphMLTokens.KEY);
                writer.writeAttribute(GraphMLTokens.ID, key);
                writer.writeAttribute(GraphMLTokens.FOR, GraphMLTokens.EDGE);
                writer.writeAttribute(GraphMLTokens.ATTR_NAME, key);
                writer.writeAttribute(GraphMLTokens.ATTR_TYPE, edgeKeyTypes.get(key));
                writer.writeEndElement();
            }

            writer.writeStartElement(GraphMLTokens.GRAPH);
            writer.writeAttribute(GraphMLTokens.ID, GraphMLTokens.G);
            writer.writeAttribute(GraphMLTokens.EDGEDEFAULT, GraphMLTokens.DIRECTED);

            Iterable<Vertex> vertices;
            if (normalize) {
                vertices = new ArrayList<>();
                for (Vertex v : graph.query().vertices()) {
                    ((Collection<Vertex>) vertices).add(v);
                }
                Collections.sort((List<Vertex>) vertices, new LexicographicalElementComparator());
            } else {
                vertices = graph.query().vertices();
            }
            for (Vertex vertex : vertices) {
                writer.writeStartElement(GraphMLTokens.NODE);
                writer.writeAttribute(GraphMLTokens.ID, vertex.getId().toString());
                Collection<String> keys;
                if (normalize) {
                    keys = new ArrayList<>();
                    keys.addAll(vertex.getPropertyKeys());
                    Collections.sort((List<String>) keys);
                } else {
                    keys = vertex.getPropertyKeys();
                }
                for (String key : keys) {
                    writer.writeStartElement(GraphMLTokens.DATA);
                    writer.writeAttribute(GraphMLTokens.KEY, key);
                    Object value = vertex.getProperty(key).getValue();
                    if (null != value) {
                        writer.writeCharacters(value.toString());
                    }
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }

            if (normalize) {
                List<Edge> edges = new ArrayList<>();
                for (Vertex vertex : graph.query().vertices()) {
                    for (Edge edge : vertex.query().direction(Direction.OUT).edges()) {
                        edges.add(edge);
                    }
                }
                Collections.sort(edges, new LexicographicalElementComparator());

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
                        Object value = edge.getProperty(key).getValue();
                        if (null != value) {
                            writer.writeCharacters(value.toString());
                        }
                        writer.writeEndElement();
                    }
                    writer.writeEndElement();
                }
            } else {
                for (Vertex vertex : graph.query().vertices()) {
                    for (Edge edge : vertex.query().direction(Direction.OUT).edges()) {
                        writer.writeStartElement(GraphMLTokens.EDGE);
                        writer.writeAttribute(GraphMLTokens.ID, edge.getId().toString());
                        writer.writeAttribute(GraphMLTokens.SOURCE, edge.getVertex(Direction.OUT).getId().toString());
                        writer.writeAttribute(GraphMLTokens.TARGET, edge.getVertex(Direction.IN).getId().toString());
                        writer.writeAttribute(GraphMLTokens.LABEL, edge.getLabel());

                        for (String key : edge.getPropertyKeys()) {
                            writer.writeStartElement(GraphMLTokens.DATA);
                            writer.writeAttribute(GraphMLTokens.KEY, key);
                            Object value = edge.getProperty(key).getValue();
                            if (null != value) {
                                writer.writeCharacters(value.toString());
                            }
                            writer.writeEndElement();
                        }
                        writer.writeEndElement();
                    }
                }
            }

            writer.writeEndElement(); // graph
            writer.writeEndElement(); // graphml
            writer.writeEndDocument();

            writer.flush();
            writer.close();
        } catch (XMLStreamException xse) {
            throw new IOException(xse);
        }
    }

    private static String getStringType(final Object object) {
        if (object instanceof String) {
            return GraphMLTokens.STRING;
        } else if (object instanceof Integer) {
            return GraphMLTokens.INT;
        } else if (object instanceof Long) {
            return GraphMLTokens.LONG;
        } else if (object instanceof Float) {
            return GraphMLTokens.FLOAT;
        } else if (object instanceof Double) {
            return GraphMLTokens.DOUBLE;
        } else if (object instanceof Boolean) {
            return GraphMLTokens.BOOLEAN;
        } else {
            return GraphMLTokens.STRING;
        }
    }

    public static final class Builder {
        private final Graph g;
        private boolean normalize = false;
        private Map<String, String> vertexKeyTypes = null;
        private Map<String, String> edgeKeyTypes = null;

        private String xmlSchemaLocation = null;
        private String edgeLabelKey = null;

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

