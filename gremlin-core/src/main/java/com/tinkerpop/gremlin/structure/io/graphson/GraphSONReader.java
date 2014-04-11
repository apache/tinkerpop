package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedList;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedValue;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.util.function.QuintFunction;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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

            if (fieldName.equals(GraphSONTokens.PROPERTIES)) {
                final Map<String,Object> graphProperties = parser.readValueAs(new TypeReference<Map<String,Object>>(){});
                if (graphToWriteTo.getFeatures().graph().memory().supportsMemory())
                    graphProperties.entrySet().forEach(entry-> graphToWriteTo.memory().set(entry.getKey(), entry.getValue()));
            } else if (fieldName.equals(GraphSONTokens.VERTICES)) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    final Map<String,Object> vertexData = parser.readValueAs(new TypeReference<Map<String, Object>>() { });
                    readVertexData(vertexData, (id, label, properties) -> graph.addVertex(Stream.concat(
                            Stream.of(Element.LABEL, label, Element.ID, id),
                            Stream.of(properties)).toArray()));
                }
            } else if (fieldName.equals(GraphSONTokens.EDGES)) {
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    final Map<String,Object> edgeData = parser.readValueAs(new TypeReference<Map<String, Object>>() {});
                    readEdgeData(edgeData, (id, out, in, label, props) ->  {
                        final Vertex vOut = graph.v(out);
                        final Vertex vIn = graph.v(in);
                        // batchgraph checks for edge id support and uses it if possible.
                        return vOut.addEdge(edgeData.get(GraphSONTokens.LABEL).toString(), vIn,
                                Stream.concat(Stream.of(Element.ID, id), Stream.of(props)).toArray());
                    });
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
        return readVertexData(vertexData, vertexMaker);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final TriFunction<Object, String, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String,Object> vertexData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Vertex v = readVertexData(vertexData, vertexMaker);

        if (vertexData.containsKey(GraphSONTokens.OUT) && (direction == Direction.BOTH || direction == Direction.OUT))
            readVertexEdges(edgeMaker, vertexData, GraphSONTokens.OUT);

        if (vertexData.containsKey(GraphSONTokens.IN) && (direction == Direction.BOTH || direction == Direction.IN))
            readVertexEdges(edgeMaker, vertexData, GraphSONTokens.IN);

        return v;
    }

    private static void readVertexEdges(final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker, final Map<String, Object> vertexData, final String direction) throws IOException {
        final List<Map<String,Object>> edgeDatas = (List<Map<String,Object>>) vertexData.get(direction);
        for (Map<String,Object> edgeData : edgeDatas) {
            readEdgeData(edgeData, edgeMaker);
        }
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String,Object> edgeData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        return readEdgeData(edgeData, edgeMaker);
    }

    private static Edge readEdgeData(final Map<String,Object> edgeData, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String, Object> properties = (Map<String, Object>) edgeData.get(GraphSONTokens.PROPERTIES);
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue())).toArray();
        return edgeMaker.apply(
                edgeData.get(GraphSONTokens.ID),
                edgeData.get(GraphSONTokens.OUT),
                edgeData.get(GraphSONTokens.IN),
                edgeData.get(GraphSONTokens.LABEL).toString(),
                propsAsArray);
    }

    private static Vertex readVertexData(final Map<String,Object> vertexData, final TriFunction<Object, String, Object[], Vertex> vertexMaker) throws IOException {
        final Map<String, Object> properties = (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES);
        final List<Pair<String, IOAnnotatedList>> annotatedLists = new ArrayList<>();
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e -> {
            final Object o = e.getValue();
            if (o instanceof IOAnnotatedList) {
                annotatedLists.add(Pair.with(e.getKey(), (IOAnnotatedList) o));
                return Stream.of(e.getKey(), AnnotatedList.make());
            } else {
                return Stream.of(e.getKey(), e.getValue());
            }
        }).toArray();

        final Vertex newVertex = vertexMaker.apply(vertexData.get(GraphSONTokens.ID), vertexData.get(GraphSONTokens.LABEL).toString(), propsAsArray);
        setAnnotatedListValues(annotatedLists, newVertex);
        return newVertex;
    }

    private static void setAnnotatedListValues(final List<Pair<String, IOAnnotatedList>> annotatedLists, final Vertex v) {
        annotatedLists.forEach(kal -> {
            final AnnotatedList al = v.getValue(kal.getValue0());
            final List<IOAnnotatedValue> valuesForAnnotation = kal.getValue1().getAnnotatedValueList();
            for (IOAnnotatedValue kav : valuesForAnnotation) {
                al.addValue(kav.getValue(), kav.toAnnotationsArray());
            }
        });
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private boolean loadCustomModules = false;
        private SimpleModule custom = null;
        private long batchSize = BatchGraph.DEFAULT_BUFFER_SIZE;
        private GraphSONObjectMapper.TypeEmbedding typeEmbedding = GraphSONObjectMapper.TypeEmbedding.NONE;

        private Builder() {}

        /**
         * Supply a custom module for serialization/deserialization.
         */
        public Builder customModule(final SimpleModule custom) {
            this.custom = custom;
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #customModule(com.fasterxml.jackson.databind.module.SimpleModule)};
         */
        public Builder loadCustomModules(final boolean loadCustomModules) {
            this.loadCustomModules = loadCustomModules;
            return this;
        }

        public Builder typeEmbedding(final GraphSONObjectMapper.TypeEmbedding typeEmbedding) {
            this.typeEmbedding = typeEmbedding;
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
                    .customModule(custom)
                    .typeEmbedding(typeEmbedding)
                    .loadCustomModules(loadCustomModules).build();
            return new GraphSONReader(mapper, batchSize);
        }
    }
}
