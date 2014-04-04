package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.util.function.QuintFunction;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A @{link GraphReader} that constructs a graph from a JSON-based representation of a graph and its elements.
 * This implementation only supports JSON data types and is therefore lossy with respect to data types (e.g. a
 * float will become a double, element IDs may not be retrieved in the format they were serialized, etc.).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONReader implements GraphReader {
    private final ObjectMapper mapper;
    private final long batchSize;

    public GraphSONReader(final ObjectMapper mapper, final long batchSize) {
        this.mapper = mapper;
        this.batchSize = batchSize;
    }

    // todo: do we allow custom deserializers for ID and properties mapping property keys to data types?  would make graphson configurably lossy?

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        final JsonFactory factory = mapper.getFactory();
        final JsonParser parser = factory.createParser(inputStream);
        final BatchGraph graph = new BatchGraph.Builder<>(graphToWriteTo)
                .bufferSize(batchSize).build();

        if (parser.nextToken() != JsonToken.START_OBJECT)
            throw new IOException("Expected data to start with an Object");

        while (parser.nextToken() != JsonToken.END_OBJECT) {
            final String fieldName = parser.getCurrentName();
            parser.nextToken();

            if (fieldName.equals(GraphSONTokens.TOKEN_PROPERTIES)) {
                final Map<String,Object> graphProperties = parser.readValueAs(new TypeReference<Map<String,Object>>(){});
                if (graphToWriteTo.getFeatures().graph().memory().supportsMemory())
                    graphProperties.entrySet().forEach(entry-> graphToWriteTo.memory().set(entry.getKey(), entry.getValue()));
            } else if (fieldName.equals(GraphSONTokens.TOKEN_VERTICES)) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    final Map<String,Object> vertexData = parser.readValueAs(new TypeReference<Map<String, Object>>() { });
                    final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) vertexData.get(GraphSONTokens.TOKEN_PROPERTIES);
                    final Object[] propsAsArray = Stream.concat(properties.entrySet().stream().flatMap(e->Stream.of(e.getKey(), e.getValue().get("value"))),
                            Stream.of(Element.LABEL, vertexData.get(GraphSONTokens.TOKEN_LABEL), Element.ID, vertexData.get(GraphSONTokens.TOKEN_ID))).toArray();
                    graph.addVertex(propsAsArray);
                }
            } else if (fieldName.equals(GraphSONTokens.TOKEN_EDGES)) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    final Map<String,Object> edgeData = parser.readValueAs(new TypeReference<Map<String, Object>>() {});
                    final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) edgeData.get(GraphSONTokens.TOKEN_PROPERTIES);
                    final Object[] propsAsArray = Stream.concat(properties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))),
                            Stream.of(Element.ID, edgeData.get(GraphSONTokens.TOKEN_ID))).toArray();
                    final Vertex vOut = graph.v(edgeData.get(GraphSONTokens.TOKEN_OUT));
                    final Vertex vIn = graph.v(edgeData.get(GraphSONTokens.TOKEN_IN));
                    vOut.addEdge(edgeData.get(GraphSONTokens.TOKEN_LABEL).toString(), vIn, propsAsArray);
                }
            } else
                throw new IllegalStateException(String.format("Unexpected token in GraphSON - %s", fieldName));
        }

        graph.tx().commit();
        parser.close();
    }

    @Override
    public Vertex readVertex(final InputStream inputStream,
                             final TriFunction<Object, String, Object[], Vertex> vertexMaker) throws IOException {
        final Map<String,Object> vertexData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) vertexData.get(GraphSONTokens.TOKEN_PROPERTIES);
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
        return vertexMaker.apply(vertexData.get(GraphSONTokens.TOKEN_ID), vertexData.get(GraphSONTokens.TOKEN_LABEL).toString(), propsAsArray);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final TriFunction<Object, String, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String,Object> vertexData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) vertexData.get(GraphSONTokens.TOKEN_PROPERTIES);
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e->Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
        final Vertex v = vertexMaker.apply(vertexData.get(GraphSONTokens.TOKEN_ID), vertexData.get(GraphSONTokens.TOKEN_LABEL).toString(), propsAsArray);

        if (vertexData.containsKey(GraphSONTokens.TOKEN_OUT) && (direction == Direction.BOTH || direction == Direction.OUT))
            readVertexOutEdges(edgeMaker, vertexData);

        if (vertexData.containsKey(GraphSONTokens.TOKEN_IN) && (direction == Direction.BOTH || direction == Direction.IN))
            readVertexInEdges(edgeMaker, vertexData);

        return v;
    }

    private static void readVertexInEdges(final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker, final Map<String, Object> vertexData) {
        final List<Map<String,Object>> edgeDatas = (List<Map<String,Object>>) vertexData.get(GraphSONTokens.TOKEN_IN);
        for (Map<String,Object> edgeData : edgeDatas) {
            final Map<String, Map<String, Object>> edgeProperties = (Map<String,Map<String, Object>>) edgeData.get(GraphSONTokens.TOKEN_PROPERTIES);
            final Object[] edgePropsAsArray = edgeProperties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
            edgeMaker.apply(
                    edgeData.get(GraphSONTokens.TOKEN_ID),
                    edgeData.get(GraphSONTokens.TOKEN_OUT),
                    edgeData.get(GraphSONTokens.TOKEN_IN),
                    edgeData.get(GraphSONTokens.TOKEN_LABEL).toString(),
                    edgePropsAsArray);
        }
    }

    private static void readVertexOutEdges(final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker, final Map<String, Object> vertexData) {
        final List<Map<String,Object>> edgeDatas = (List<Map<String,Object>>) vertexData.get(GraphSONTokens.TOKEN_OUT);
        for (Map<String,Object> edgeData : edgeDatas) {
            final Map<String, Map<String, Object>> edgeProperties = (Map<String,Map<String, Object>>) edgeData.get(GraphSONTokens.TOKEN_PROPERTIES);
            final Object[] edgePropsAsArray = edgeProperties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
            edgeMaker.apply(
                    edgeData.get(GraphSONTokens.TOKEN_ID),
                    edgeData.get(GraphSONTokens.TOKEN_OUT),
                    edgeData.get(GraphSONTokens.TOKEN_IN),
                    edgeData.get(GraphSONTokens.TOKEN_LABEL).toString(),
                    edgePropsAsArray);
        }
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String,Object> edgeData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) edgeData.get(GraphSONTokens.TOKEN_PROPERTIES);
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
        return edgeMaker.apply(
                edgeData.get(GraphSONTokens.TOKEN_ID),
                edgeData.get(GraphSONTokens.TOKEN_OUT),
                edgeData.get(GraphSONTokens.TOKEN_IN),
                edgeData.get(GraphSONTokens.TOKEN_LABEL).toString(),
                propsAsArray);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private boolean loadCustomSerializers = false;
        private SimpleModule custom = null;
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;

        private Builder() {}

        /**
         * Specify a custom serializer module to handle types beyond those supported generally by TinkerPop.
         */
        public Builder customSerializer(final SimpleModule module) {
            this.custom = module;
            return this;
        }

        /**
         * Attempt to load external custom serialization modules from the class path.
         */
        public Builder loadCustomSerializers(final boolean loadCustomSerializers) {
            this.loadCustomSerializers = loadCustomSerializers;
            return this;
        }

        /**
         * Number of mutations to perform before a commit is executed.
         */
        public Builder batchSize(final long batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public GraphSONReader build() {
            final ObjectMapper mapper = GraphSONObjectMapper.create()
                    .customSerializer(custom).loadCustomSerializers(loadCustomSerializers).build();
            return new GraphSONReader(mapper, batchSize);
        }
    }
}
