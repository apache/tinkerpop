/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Serialization and deserialization tests for GraphSON V3 with the TinkerGraph custom type.
 */
public class TinkerGraphGraphSONSerializerV3Test {

    private final Mapper defaultMapperV3 = GraphSONMapper.build()
            .version(GraphSONVersion.V3_0)
            .addCustomModule(GraphSONXModuleV3.build())
            .addRegistry(TinkerIoRegistryV3.instance())
            .create();

    @Test
    public void shouldDeserializeWellFormedGraph() throws IOException {
        final TinkerGraph original = TinkerFactory.createModern();
        final GraphWriter writer = getWriter(defaultMapperV3);
        final GraphReader reader = getReader(defaultMapperV3);

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeObject(out, original);
            final String json = out.toString();
            final TinkerGraph read = reader.readObject(new ByteArrayInputStream(json.getBytes()), TinkerGraph.class);
            assertEquals(6L, read.traversal().V().count().next().longValue());
            assertEquals(6L, read.traversal().E().count().next().longValue());
        }
    }

    @Test(timeout = 5000)
    public void shouldFailFastOnScalarVerticesField() throws IOException {
        final String malformed = "{\"@type\":\"tinker:graph\",\"@value\":{\"vertices\":0}}";
        final GraphReader reader = getReader(defaultMapperV3);
        try {
            reader.readObject(new ByteArrayInputStream(malformed.getBytes()), TinkerGraph.class);
            fail("Expected IOException for malformed tinker:graph input");
        } catch (IOException expected) {
            // JsonParseException — the START_ARRAY check threw as intended
        }
    }

    @Test(timeout = 5000)
    public void shouldFailFastOnScalarEdgesField() throws IOException {
        final String malformed = "{\"@type\":\"tinker:graph\",\"@value\":{\"vertices\":[],\"edges\":0}}";
        final GraphReader reader = getReader(defaultMapperV3);
        try {
            reader.readObject(new ByteArrayInputStream(malformed.getBytes()), TinkerGraph.class);
            fail("Expected IOException for malformed tinker:graph input");
        } catch (IOException expected) {
            // JsonParseException — the START_ARRAY check threw as intended
        }
    }

    @Test(timeout = 5000)
    public void shouldFailFastOnTruncatedInput() throws IOException {
        final String malformed = "{\"@type\":\"tinker:graph\",\"@value\":{\"vertices\":[";
        final GraphReader reader = getReader(defaultMapperV3);
        try {
            reader.readObject(new ByteArrayInputStream(malformed.getBytes()), TinkerGraph.class);
            fail("Expected IOException for truncated tinker:graph input");
        } catch (IOException expected) {
            // JsonParseException — nextTokenOrThrow detected end-of-input
        }
    }

    private GraphWriter getWriter(Mapper paramMapper) {
        return GraphSONWriter.build().mapper(paramMapper).create();
    }

    private GraphReader getReader(Mapper paramMapper) {
        return GraphSONReader.build().mapper(paramMapper).create();
    }
}
