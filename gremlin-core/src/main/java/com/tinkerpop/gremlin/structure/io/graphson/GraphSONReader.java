package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.util.function.QuintFunction;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONReader implements GraphReader {
    private final Graph g;
    private final ObjectMapper mapper;

    public GraphSONReader(final Graph g, final ObjectMapper mapper) {
        this.g = g;
        this.mapper = mapper;
    }

    @Override
    public void readGraph(InputStream inputStream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex readVertex(final InputStream inputStream,
                             final BiFunction<Object, Object[], Vertex> vertexMaker) throws IOException {
        final Map<String,Object> vertexData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) vertexData.get(GraphSONModule.TOKEN_PROPERTIES);
        final Object[] propsAsArray = Stream.concat(properties.entrySet().stream().flatMap(e->Stream.of(e.getKey(), e.getValue().get("value"))),
                Stream.of(Element.LABEL, vertexData.get(GraphSONModule.TOKEN_LABEL))).toArray();
        return vertexMaker.apply(vertexData.get(GraphSONModule.TOKEN_ID), propsAsArray);
    }

    @Override
    public Vertex readVertex(final InputStream inputStream, final Direction direction,
                             final BiFunction<Object, Object[], Vertex> vertexMaker,
                             final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Edge readEdge(final InputStream inputStream, final QuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker) throws IOException {
        final Map<String,Object> edgeData = mapper.readValue(inputStream, new TypeReference<Map<String,Object>>(){});
        final Map<String, Map<String, Object>> properties = (Map<String,Map<String, Object>>) edgeData.get(GraphSONModule.TOKEN_PROPERTIES);
        final Object[] propsAsArray = properties.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue().get("value"))).toArray();
        return edgeMaker.apply(
                edgeData.get(GraphSONModule.TOKEN_ID),
                edgeData.get(GraphSONModule.TOKEN_OUT),
                edgeData.get(GraphSONModule.TOKEN_IN),
                edgeData.get(GraphSONModule.TOKEN_LABEL).toString(),
                propsAsArray);
    }

    public static class Builder {
        private final Graph g;
        private ObjectMapper mapper = new GraphSONObjectMapper();

        public Builder(final Graph g) {
            this.g = g;
        }

        public Builder customSerializer(final SimpleModule module) {
            this.mapper = new GraphSONObjectMapper(
                    Optional.ofNullable(module).orElseThrow(IllegalArgumentException::new));
            return this;
        }

        public GraphSONReader build() {
            return new GraphSONReader(g, mapper);
        }
    }
}
