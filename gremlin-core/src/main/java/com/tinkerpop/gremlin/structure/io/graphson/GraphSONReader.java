package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A @{link GraphReader} that constructs a graph from a JSON-based representation of a graph and its elements.
 * This implementation only supports JSON data types and is therefore lossy with respect to data types (e.g. a
 * float will become a double, element IDs may not be retrieved in the format they were serialized, etc.).
 * {@link Edge} and {@link Vertex} objects are serialized to {@code Map} instances.  If an
 * {@link com.tinkerpop.gremlin.structure.Element} is used as a key, it is coerced to its identifier.  Other complex
 * objects are converted via {@link Object#toString()} unless there is a mapper serializer supplied.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONReader implements GraphReader {
    private final ObjectMapper mapper;
    private final long batchSize;
    private final String vertexIdKey;
    private final String edgeIdKey;

    final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
    };

    public GraphSONReader(final GraphSONMapper mapper, final long batchSize,
                          final String vertexIdKey, final String edgeIdKey) {
        this.mapper = mapper.createMapper();
        this.batchSize = batchSize;
        this.vertexIdKey = vertexIdKey;
        this.edgeIdKey = edgeIdKey;
    }

    @Override
    public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
        final BatchGraph graph;
        try {
            // will throw an exception if not constructed properly
            graph = BatchGraph.build(graphToWriteTo)
                    .vertexIdKey(vertexIdKey)
                    .edgeIdKey(edgeIdKey)
                    .bufferSize(batchSize).create();
        } catch (Exception ex) {
            throw new IOException("Could not instantiate BatchGraph wrapper", ex);
        }

        final JsonFactory factory = mapper.getFactory();

        try (JsonParser parser = factory.createParser(inputStream)) {
            if (parser.nextToken() != JsonToken.START_OBJECT)
                throw new IOException("Expected data to start with an Object");

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                final String fieldName = parser.getCurrentName();
                parser.nextToken();

                if (fieldName.equals(GraphSONTokens.VARIABLES)) {
                    final Map<String, Object> graphVariables = parser.readValueAs(mapTypeReference);
                    if (graphToWriteTo.features().graph().variables().supportsVariables())
                        graphVariables.entrySet().forEach(entry -> graphToWriteTo.variables().set(entry.getKey(), entry.getValue()));
                } else if (fieldName.equals(GraphSONTokens.VERTICES)) {
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        final Map<String, Object> vertexData = parser.readValueAs(mapTypeReference);
                        readVertexData(vertexData, detachedVertex -> {
                            final Iterator<Vertex> iterator = graph.iterators().vertexIterator(detachedVertex.id());
                            final Vertex v = iterator.hasNext() ? iterator.next() : graph.addVertex(T.label, detachedVertex.label(), T.id, detachedVertex.id());
                            detachedVertex.iterators().propertyIterator().forEachRemaining(p -> createVertexProperty(graphToWriteTo, v, p, false));
                            return v;
                        });
                    }
                } else if (fieldName.equals(GraphSONTokens.EDGES)) {
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        final Map<String, Object> edgeData = parser.readValueAs(mapTypeReference);
                        readEdgeData(edgeData, detachedEdge -> {
                            final Vertex vOut = graph.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.OUT).next().id()).next();
                            final Vertex vIn = graph.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.IN).next().id()).next();
                            // batchgraph checks for edge id support and uses it if possible.
                            final Edge e = vOut.addEdge(edgeData.get(GraphSONTokens.LABEL).toString(), vIn, T.id, detachedEdge.id());
                            detachedEdge.iterators().propertyIterator().forEachRemaining(p -> e.<Object>property(p.key(), p.value()));
                            return e;
                        });
                    }
                } else
                    throw new IllegalStateException(String.format("Unexpected token in GraphSON - %s", fieldName));
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
        final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        return br.lines().<Vertex>map(FunctionUtils.wrapFunction(line -> readVertex(new ByteArrayInputStream(line.getBytes()), direction, vertexMaker, edgeMaker))).iterator();
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Map<String, Object> edgeData = mapper.readValue(inputStream, mapTypeReference);
        return readEdgeData(edgeData, edgeMaker);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Function<DetachedVertex, Vertex> vertexMaker) throws IOException {
        final Map<String, Object> vertexData = mapper.readValue(inputStream, mapTypeReference);
        return readVertexData(vertexData, vertexMaker);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final Function<DetachedVertex, Vertex> vertexMaker,
                             final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Map<String, Object> vertexData = mapper.readValue(inputStream, mapTypeReference);
        final Vertex v = readVertexData(vertexData, vertexMaker);

        if (vertexData.containsKey(GraphSONTokens.OUT_E) && (direction == Direction.BOTH || direction == Direction.OUT))
            readVertexEdges(edgeMaker, vertexData, GraphSONTokens.OUT_E);

        if (vertexData.containsKey(GraphSONTokens.IN_E) && (direction == Direction.BOTH || direction == Direction.IN))
            readVertexEdges(edgeMaker, vertexData, GraphSONTokens.IN_E);

        return v;
    }

    private static void createVertexProperty(final Graph graphToWriteTo, final Vertex v, final VertexProperty<Object> p, final boolean hidden) {
        final List<Object> propertyArgs = new ArrayList<>();
        if (graphToWriteTo.features().vertex().properties().supportsUserSuppliedIds())
            propertyArgs.addAll(Arrays.asList(T.id, p.id()));
        p.iterators().propertyIterator().forEachRemaining(it -> propertyArgs.addAll(Arrays.asList(it.key(), it.value())));
        v.property(p.key(), p.value(), propertyArgs.toArray());
    }

    private static void readVertexEdges(final Function<DetachedEdge, Edge> edgeMaker, final Map<String, Object> vertexData, final String direction) throws IOException {
        final List<Map<String, Object>> edgeDatas = (List<Map<String, Object>>) vertexData.get(direction);
        for (Map<String, Object> edgeData : edgeDatas) {
            readEdgeData(edgeData, edgeMaker);
        }
    }

    private static Edge readEdgeData(final Map<String, Object> edgeData, final Function<DetachedEdge, Edge> edgeMaker) throws IOException {
        final Map<String, Object> properties = (Map<String, Object>) edgeData.get(GraphSONTokens.PROPERTIES);

        final DetachedEdge edge = new DetachedEdge(edgeData.get(GraphSONTokens.ID),
                edgeData.get(GraphSONTokens.LABEL).toString(),
                properties,
                Pair.with(edgeData.get(GraphSONTokens.OUT), edgeData.get(GraphSONTokens.OUT_LABEL).toString()),
                Pair.with(edgeData.get(GraphSONTokens.IN), edgeData.get(GraphSONTokens.IN_LABEL).toString()));

        return edgeMaker.apply(edge);
    }

    private static Vertex readVertexData(final Map<String, Object> vertexData, final Function<DetachedVertex, Vertex> vertexMaker) throws IOException {
        final Map<String, Object> vertexProperties = (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES);
        final DetachedVertex vertex = new DetachedVertex(vertexData.get(GraphSONTokens.ID),
                vertexData.get(GraphSONTokens.LABEL).toString(),
                vertexProperties);

        return vertexMaker.apply(vertex);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;
        private String vertexIdKey = T.id.getAccessor();
        private String edgeIdKey = T.id.getAccessor();

        private GraphSONMapper mapper = GraphSONMapper.build().create();

        private Builder() {
        }

        /**
         * The name of the key to supply to
         * {@link com.tinkerpop.gremlin.structure.util.batch.BatchGraph.Builder#vertexIdKey} when reading data into
         * the {@link Graph}.
         */
        public Builder vertexIdKey(final String vertexIdKey) {
            this.vertexIdKey = vertexIdKey;
            return this;
        }

        /**
         * The name of the key to supply to
         * {@link com.tinkerpop.gremlin.structure.util.batch.BatchGraph.Builder#edgeIdKey} when reading data into
         * the {@link Graph}.
         */
        public Builder edgeIdKey(final String edgeIdKey) {
            this.edgeIdKey = edgeIdKey;
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
         * Override all of the {@link GraphSONMapper} builder
         * options with this mapper.  If this value is set to something other than null then that value will be
         * used to construct the writer.
         */
        public Builder mapper(final GraphSONMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public GraphSONReader create() {
            return new GraphSONReader(mapper, batchSize, vertexIdKey, edgeIdKey);
        }
    }
}
