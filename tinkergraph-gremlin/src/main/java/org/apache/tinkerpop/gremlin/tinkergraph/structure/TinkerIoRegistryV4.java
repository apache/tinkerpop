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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of the {@link IoRegistry} interface that provides serializers with custom configurations for
 * implementation specific classes that might need to be serialized.  This registry allows a {@link TinkerGraph} to
 * be serialized directly which is useful for moving small graphs around on the network.
 * <p/>
 * Most providers need not implement this kind of custom serializer as they will deal with much larger graphs that
 * wouldn't be practical to serialize in this fashion.  This is a bit of a special case for TinkerGraph given its
 * in-memory status.  Typical implementations would create serializers for a complex vertex identifier or a
 * custom data class like a "geographic point".
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerIoRegistryV4 extends AbstractIoRegistry {

    private static final TinkerIoRegistryV4 INSTANCE = new TinkerIoRegistryV4();

    private TinkerIoRegistryV4() {
        register(GryoIo.class, TinkerGraph.class, new TinkerGraphGryoSerializer());
        register(GraphSONIo.class, null, new TinkerModuleV2());
    }

    public static TinkerIoRegistryV4 instance() {
        return INSTANCE;
    }

    /**
     * Provides a method to serialize an entire {@link TinkerGraph} into itself for Gryo.  This is useful when
     * shipping small graphs around through Gremlin Server. Reuses the existing Kryo instance for serialization.
     */
    final static class TinkerGraphGryoSerializer extends Serializer<TinkerGraph> {
        @Override
        public void write(final Kryo kryo, final Output output, final TinkerGraph graph) {
            try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                GryoWriter.build().mapper(() -> kryo).create().writeGraph(stream, graph);
                final byte[] bytes = stream.toByteArray();
                output.writeInt(bytes.length);
                output.write(bytes);
            } catch (Exception io) {
                throw new RuntimeException(io);
            }
        }

        @Override
        public TinkerGraph read(final Kryo kryo, final Input input, final Class<TinkerGraph> tinkerGraphClass) {
            final Configuration conf = new BaseConfiguration();
            conf.setProperty("gremlin.tinkergraph.defaultVertexPropertyCardinality", "list");
            final TinkerGraph graph = TinkerGraph.open(conf);
            final int len = input.readInt();
            final byte[] bytes = input.readBytes(len);
            try (final ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                GryoReader.build().mapper(() -> kryo).create().readGraph(stream, graph);
            } catch (Exception io) {
                throw new RuntimeException(io);
            }

            return graph;
        }
    }

    /**
     * Provides a method to serialize an entire {@link TinkerGraph} into itself for GraphSON. This is useful when
     * shipping small graphs around through Gremlin Server.
     */
    final static class TinkerModuleV2 extends TinkerPopJacksonModule {
        public TinkerModuleV2() {
            super("tinkergraph-2.0");
            addSerializer(TinkerGraph.class, new TinkerGraphJacksonSerializer());
            addDeserializer(TinkerGraph.class, new TinkerGraphJacksonDeserializer());
        }

        @Override
        public Map<Class, String> getTypeDefinitions() {
            return new HashMap<Class, String>(){{
                put(TinkerGraph.class, "graph");
            }};
        }

        @Override
        public String getTypeNamespace() {
            return GraphSONTokens.GREMLIN_TYPE_NAMESPACE;
        }
    }

    /**
     * Serializes the graph into an edge list format.  Edge list is a better choices than adjacency list (which is
     * typically standard from the {@link GraphReader} and {@link GraphWriter} perspective) in this case because
     * the use case for this isn't around massive graphs.  The use case is for "small" subgraphs that are being
     * shipped over the wire from Gremlin Server. Edge list format is a bit easier for non-JVM languages to work
     * with as a format and doesn't require a cache for loading (as vertex labels are not serialized in adjacency
     * list).
     */
    final static class TinkerGraphJacksonSerializer extends StdScalarSerializer<TinkerGraph> {

        public TinkerGraphJacksonSerializer() {
            super(TinkerGraph.class);
        }

        @Override
        public void serialize(final TinkerGraph graph, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName(GraphSONTokens.VERTICES);
            jsonGenerator.writeStartArray();

            final Iterator<Vertex> vertices = graph.vertices();
            while (vertices.hasNext()) {
                serializerProvider.defaultSerializeValue(vertices.next(), jsonGenerator);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeFieldName(GraphSONTokens.EDGES);
            jsonGenerator.writeStartArray();

            final Iterator<Edge> edges = graph.edges();
            while (edges.hasNext()) {
                serializerProvider.defaultSerializeValue(edges.next(), jsonGenerator);
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
        }
    }

    /**
     * Deserializes the edge list format.
     */
    static class TinkerGraphJacksonDeserializer extends StdDeserializer<TinkerGraph> {
        public TinkerGraphJacksonDeserializer() {
            super(TinkerGraph.class);
        }

        @Override
        public TinkerGraph deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Configuration conf = new BaseConfiguration();
            conf.setProperty("gremlin.tinkergraph.defaultVertexPropertyCardinality", "list");
            final TinkerGraph graph = TinkerGraph.open(conf);

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals("vertices")) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                            final DetachedVertex v = (DetachedVertex) deserializationContext.readValue(jsonParser, Vertex.class);
                            v.attach(Attachable.Method.getOrCreate(graph));
                        }
                    }
                } else if (jsonParser.getCurrentName().equals("edges")) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                            final DetachedEdge e = (DetachedEdge) deserializationContext.readValue(jsonParser, Edge.class);
                            e.attach(Attachable.Method.getOrCreate(graph));
                        }
                    }
                }
            }

            return graph;
        }
    }
}
