package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.Comparators;
import com.tinkerpop.gremlin.util.function.FunctionUtils;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphSONGraph {
    private final Graph graphToSerialize;

    public GraphSONGraph(final Graph graphToSerialize) {
        this.graphToSerialize = graphToSerialize;
    }

    public Graph getGraphToSerialize() {
        return graphToSerialize;
    }

    static class GraphJacksonSerializer extends StdSerializer<GraphSONGraph> {
        private final boolean normalize;

        public GraphJacksonSerializer(final boolean normalize) {
            super(GraphSONGraph.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final GraphSONGraph graph, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            final Graph g = graph.getGraphToSerialize();
            jsonGenerator.writeStartObject();

            if (g.getFeatures().graph().memory().supportsMemory())
                if (normalize) {
                    jsonGenerator.writeObjectFieldStart(GraphSONTokens.TOKEN_PROPERTIES);
                    g.memory().asMap().entrySet().stream().sorted(Comparators.OBJECT_ENTRY_COMPARATOR)
                            .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.getValue())));
                    jsonGenerator.writeEndObject();
                } else
                    jsonGenerator.writeObjectField(GraphSONTokens.TOKEN_PROPERTIES, g.memory().asMap());

            jsonGenerator.writeArrayFieldStart(GraphSONTokens.TOKEN_VERTICES);
            if (normalize)
                g.V().order(Comparators.HELD_VERTEX_COMPARATOR).forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            else
                g.V().forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            jsonGenerator.writeEndArray();

            jsonGenerator.writeArrayFieldStart(GraphSONTokens.TOKEN_EDGES);
            if (normalize)
                g.E().order(Comparators.HELD_EDGE_COMPARATOR).forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            else
                g.E().forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            jsonGenerator.writeEndArray();

            jsonGenerator.writeEndObject();
        }
    }
}
