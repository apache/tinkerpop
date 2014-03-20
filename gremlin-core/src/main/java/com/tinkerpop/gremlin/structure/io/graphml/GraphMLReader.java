package com.tinkerpop.gremlin.structure.io.graphml;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.util.function.QuintFunction;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * GraphMLReader writes the data from a GraphML stream to a graph.  Note that this format is lossy, in the sense that data
 * types and features of Gremlin Structure not supported by GraphML are not serialized.  This format is meant for
 * external export of a graph to tools outside of Gremlin Structure graphs.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Alex Averbuch (alex.averbuch@gmail.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMLReader implements GraphReader {
    public static final int DEFAULT_BATCH_SIZE = 1000;
    private final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    private final Graph graphToWriteTo;

    private final Optional<String> vertexIdKey;
    private final Optional<String> edgeIdKey;
    private final Optional<String> edgeLabelKey;
    private final int batchSize;

    private GraphMLReader(final Graph graph, final String vertexIdKey, final String edgeIdKey,
                          final String edgeLabelKey, final int batchSize) {
        this.graphToWriteTo = graph;
        this.vertexIdKey = Optional.ofNullable(vertexIdKey);
        this.edgeIdKey = Optional.ofNullable(edgeIdKey);
        this.edgeLabelKey = Optional.ofNullable(edgeLabelKey);
        this.batchSize = batchSize;
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final BiFunction<Object, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final BiFunction<Object, Object[], Vertex> vertexMaker) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
    }

    @Override
    public void readGraph(final InputStream graphInputStream) throws IOException {
        try {
            final XMLStreamReader reader = inputFactory.createXMLStreamReader(graphInputStream);

            // todo: get BatchGraph in here when TinkerPop3 has it
            final BatchGraph graph = new BatchGraph.Builder<>(graphToWriteTo)
                    .bufferSize(batchSize).build();

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
                                edgeOutVertex = Optional.ofNullable(graph.v(vertexIdOut))
                                        .orElseGet(() -> graph.addVertex(Element.ID, vertexIdOut));
                            else
                                edgeOutVertex = Optional.ofNullable(graph.v(vertexMappedIdMap.get(vertexIdOut)))
                                        .orElseGet(() -> graph.addVertex(Element.ID, vertexIdOut));

                            // Default to standard ID system (in case no mapped ID is found later)
                            if (vertexIdKey.isPresent())
                                vertexMappedIdMap.put(vertexIdOut, vertexIdOut);

                            if (!vertexIdKey.isPresent())
                                edgeInVertex = Optional.ofNullable(graph.v(vertexIdIn))
                                        .orElseGet(() -> graph.addVertex(Element.ID, vertexIdIn));
                            else
                                edgeInVertex = Optional.ofNullable(graph.v(vertexMappedIdMap.get(vertexIdIn)))
                                        .orElseGet(() -> graph.addVertex(Element.ID, vertexIdIn));

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
                        final Vertex currentVertex = Optional.ofNullable(graph.v(vertexId))
                                .orElseGet(() -> graph.addVertex(Element.ID, currentVertexId));
                        for (Map.Entry<String, Object> prop : vertexProps.entrySet()) {
                            currentVertex.setProperty(prop.getKey(), prop.getValue());
                        }

                        vertexId = null;
                        vertexProps = null;
                        isInVertex = false;
                    } else if (elementName.equals(GraphMLTokens.EDGE)) {
                        final Edge currentEdge = edgeOutVertex.addEdge(edgeLabel, edgeInVertex, Element.ID, edgeId);
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

            graph.tx().commit();
        } catch (XMLStreamException xse) {
            throw new IOException(xse);
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

    /**
     * Allows configuration and construction of the GraphMLReader instance.
     */
    public static final class Builder {
        private final Graph g;
        private String vertexIdKey = null;
        private String edgeIdKey = null;
        private String edgeLabelKey = null;
        private int batchSize = DEFAULT_BATCH_SIZE;

        public Builder(final Graph g) {
            if (null == g)
                throw new IllegalArgumentException("Graph argument cannot be null");

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
