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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;

import java.io.IOException;
import java.util.HashMap;

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
        public void serialize(final GraphSONGraph graphSONGraph, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(graphSONGraph, jsonGenerator);
        }

        @Override
        public void serializeWithType(final GraphSONGraph graphSONGraph, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(graphSONGraph, jsonGenerator);
        }

        private void ser(final GraphSONGraph graphSONGraph, final JsonGenerator jsonGenerator) throws IOException {
            final Graph g = graphSONGraph.getGraphToSerialize();
            jsonGenerator.writeStartObject();

            if (g.features().graph().variables().supportsVariables())
                jsonGenerator.writeObjectField(GraphSONTokens.VARIABLES, new HashMap<>(g.variables().asMap()));

            jsonGenerator.writeArrayFieldStart(GraphSONTokens.VERTICES);
            if (normalize)
                g.V().order().by(Comparators.VERTEX_COMPARATOR).forEachRemaining(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            else
                g.iterators().vertexIterator().forEachRemaining(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            jsonGenerator.writeEndArray();

            jsonGenerator.writeArrayFieldStart(GraphSONTokens.EDGES);
            if (normalize)
                g.E().order().by(Comparators.EDGE_COMPARATOR).forEachRemaining(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            else
                g.iterators().edgeIterator().forEachRemaining(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
            jsonGenerator.writeEndArray();

            jsonGenerator.writeEndObject();
        }
    }
}
