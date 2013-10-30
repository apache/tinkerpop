package com.tinkerpop.blueprints.io.graphml;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.io.GraphReader;
import com.tinkerpop.blueprints.util.StreamFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMLReader implements GraphReader {
    public static final int DEFAULT_BATCH_SIZE = 1000;
    private final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    private final Graph graph;

    private final Optional<String> vertexIdKey;
    private final Optional<String> edgeIdKey;
    private final Optional<String> edgeLabelKey;
    private final int batchSize;

    private GraphMLReader(final Graph graph, final String vertexIdKey, final String edgeIdKey,
                          final String edgeLabelKey, final int batchSize) {
        this.graph = graph;
        this.vertexIdKey = Optional.ofNullable(vertexIdKey);
        this.edgeIdKey = Optional.ofNullable(edgeIdKey);
        this.edgeLabelKey = Optional.ofNullable(edgeLabelKey);
        this.batchSize = batchSize;
    }

    @Override
    public void inputGraph(final InputStream graphInputStream) throws IOException {
        try {
            final XMLStreamReader reader = inputFactory.createXMLStreamReader(graphInputStream);

            // todo: get BatchGraph in here when TinkerPop3 has it
            //final BatchGraph graph = BatchGraph.wrap(inputGraph, batchSize);

            final Map<String, String> keyIdMap = new HashMap<>();
            final Map<String, String> keyTypesMaps = new HashMap<>();
            // <Mapped ID String, ID Object>

            // <Default ID String, Mapped ID String>
            final Map<String, String> vertexMappedIdMap = new HashMap<>();

            // Buffered Vertex Data
            String vertexId = null;
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
                            break;
                        case GraphMLTokens.NODE:
                            vertexId = reader.getAttributeValue(null, GraphMLTokens.ID);
                            if (vertexIdKey.isPresent())
                                vertexMappedIdMap.put(vertexId, vertexId);
                            isInVertex = true;
                            vertexProps = new HashMap<>();
                            break;
                        case GraphMLTokens.EDGE:
                            edgeId = reader.getAttributeValue(null, GraphMLTokens.ID);
                            edgeLabel = reader.getAttributeValue(null, GraphMLTokens.LABEL);
                            edgeLabel = edgeLabel == null ? GraphMLTokens._DEFAULT : edgeLabel;

                            final String vertexIdOut = reader.getAttributeValue(null, GraphMLTokens.SOURCE);
                            final String vertexIdIn = reader.getAttributeValue(null, GraphMLTokens.TARGET);

                            if (!vertexIdKey.isPresent())
                                edgeOutVertex = StreamFactory.stream(graph.query().ids(vertexIdOut).vertices()).findFirst()
                                        .orElseGet(() -> graph.addVertex(Property.of(Property.Key.ID, vertexIdOut)));
                            else
                                edgeOutVertex = StreamFactory.stream(graph.query().ids(vertexMappedIdMap.get(vertexIdOut)).vertices()).findFirst()
                                        .orElseGet(() -> graph.addVertex(Property.of(Property.Key.ID, vertexIdOut)));

                            // Default to standard ID system (in case no mapped ID is found later)
                            if (vertexIdKey.isPresent())
                                vertexMappedIdMap.put(vertexIdOut, vertexIdOut);

                            if (!vertexIdKey.isPresent())
                                edgeInVertex = StreamFactory.stream(graph.query().ids(vertexIdIn).vertices()).findFirst()
                                        .orElseGet(() -> graph.addVertex(Property.of(Property.Key.ID, vertexIdIn)));
                            else
                                edgeInVertex = StreamFactory.stream(graph.query().ids(vertexMappedIdMap.get(vertexIdIn)).vertices()).findFirst()
                                        .orElseGet(() -> graph.addVertex(Property.of(Property.Key.ID, vertexIdIn)));

                            // Default to standard ID system (in case no mapped ID is found later)
                            if (vertexIdKey.isPresent())
                                vertexMappedIdMap.put(vertexIdIn, vertexIdIn);

                            isInEdge = true;
                            edgeProps = new HashMap<>();

                            break;
                        case GraphMLTokens.DATA:
                            final String key = reader.getAttributeValue(null, GraphMLTokens.KEY);
                            final String dataAttributeName = keyIdMap.get(key);

                            if (dataAttributeName != null) {
                                final String value = reader.getElementText();

                                if (isInVertex) {
                                    if (vertexIdKey.isPresent() && key.equals(vertexIdKey.get())) {
                                        // Should occur at most once per Vertex
                                        // Assumes single ID prop per Vertex
                                        vertexMappedIdMap.put(vertexId, value);
                                        vertexId = value;
                                    } else
                                        vertexProps.put(dataAttributeName, typeCastValue(key, value, keyTypesMaps));
                                } else if (isInEdge) {
                                    if (edgeLabelKey.isPresent() && key.equals(edgeLabelKey.get()))
                                        edgeLabel = value;
                                    else if (edgeIdKey.isPresent() && key.equals(edgeIdKey.get()))
                                        edgeId = value;
                                    else
                                        edgeProps.put(dataAttributeName, typeCastValue(key, value, keyTypesMaps));
                                }
                            }

                            break;
                    }
                } else if (eventType.equals(XMLEvent.END_ELEMENT)) {
                    final String elementName = reader.getName().getLocalPart();

                    if (elementName.equals(GraphMLTokens.NODE)) {
                        final String currentVertexId = vertexId;
                        final Vertex currentVertex = StreamFactory.stream(graph.query().ids(vertexId).vertices()).findFirst()
                                .orElseGet(() -> graph.addVertex(Property.of(Property.Key.ID, currentVertexId)));
                        for (Map.Entry<String, Object> prop : vertexProps.entrySet()) {
                            currentVertex.setProperty(prop.getKey(), prop.getValue());
                        }

                        vertexId = null;
                        vertexProps = null;
                        isInVertex = false;
                    } else if (elementName.equals(GraphMLTokens.EDGE)) {
                        final Edge currentEdge = edgeOutVertex.addEdge(edgeLabel, edgeInVertex, Property.of(Property.Key.ID, edgeId));
                        for (Map.Entry<String, Object> prop : edgeProps.entrySet()) {
                            currentEdge.setProperty(prop.getKey(), prop.getValue());
                        }

                        edgeId = null;
                        edgeLabel = null;
                        edgeOutVertex = null;
                        edgeInVertex = null;
                        edgeProps = null;
                        isInEdge = false;
                    }

                }
            }

            // todo: deal with transactions when the time is right
            // graph.commit();
        } catch (XMLStreamException xse) {
            throw new IOException(xse);
        }
    }

    private static Object typeCastValue(String key, String value, Map<String, String> keyTypes) {
        String type = keyTypes.get(key);
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

    public static class Builder {
        private final Graph g;
        private String vertexIdKey = null;
        private String edgeIdKey = null;
        private String edgeLabelKey = null;
        private int batchSize = DEFAULT_BATCH_SIZE;

        public Builder(final Graph g) {
            this.g = g;
        }

        public Builder setVertexIdKey(final String vertexIdKey) {
            this.vertexIdKey = vertexIdKey;
            return this;
        }

        public Builder setEdgeIdKey(final String edgeIdKey) {
            this.edgeIdKey = edgeIdKey;
            return this;
        }

        public Builder setEdgeLabelKey(final String edgeLabelKey) {
            this.edgeLabelKey = edgeLabelKey;
            return this;
        }

        public Builder setBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public GraphMLReader build() {
            return new GraphMLReader(g, vertexIdKey, edgeIdKey, edgeLabelKey, batchSize);
        }
    }
}
