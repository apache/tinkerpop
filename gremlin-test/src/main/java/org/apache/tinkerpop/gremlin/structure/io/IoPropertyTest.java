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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoPropertyTest extends AbstractGremlinTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphson-v1", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().create()},
                {"graphson-v1-embedded", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).reader().mapper(g.io(IoCore.graphson()).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V1_0)).writer().mapper(g.io(IoCore.graphson()).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v2", false, false,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.NO_TYPES).create()).create()},
                {"graphson-v2-embedded", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V2_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V2_0)).mapper().typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()},
                {"graphson-v3", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).reader().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GraphSONIo.build(GraphSONVersion.V3_0)).writer().mapper(g.io(GraphSONIo.build(GraphSONVersion.V3_0)).mapper().create()).create()},
                {"gryo-v1", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V1_0)).writer().create()},
                {"gryo-v3", true, true,
                        (Function<Graph, GraphReader>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).reader().create(),
                        (Function<Graph, GraphWriter>) g -> g.io(GryoIo.build(GryoVersion.V3_0)).writer().create()}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public boolean assertIdDirectly;

    @Parameterized.Parameter(value = 2)
    public boolean assertDouble;

    @Parameterized.Parameter(value = 3)
    public Function<Graph, GraphReader> readerMaker;

    @Parameterized.Parameter(value = 4)
    public Function<Graph, GraphWriter> writerMaker;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldReadWriteVertexPropertyWithMetaProperties() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);

            // select any vertexproperty that has both start/end time
            final VertexProperty p = (VertexProperty) g.V(convertToVertexId("marko")).properties("location").as("p").has("endTime").select("p").next();
            writer.writeVertexProperty(os, p);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertexProperty(bais, propertyAttachable -> {
                    assertEquals(p.value(), propertyAttachable.get().value());
                    assertEquals(p.key(), propertyAttachable.get().key());
                    assertEquals(IteratorUtils.count(p.properties()), IteratorUtils.count(propertyAttachable.get().properties()));
                    assertEquals(p.property("startTime").value(), ((Property) propertyAttachable.get().properties("startTime").next()).value());
                    assertEquals(p.property("endTime").value(), ((Property) propertyAttachable.get().properties("endTime").next()).value());
                    called.set(true);
                    return propertyAttachable.get();
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWriteVertexPropertyNoMetaProperties() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            final VertexProperty p = g.V(convertToVertexId("marko")).next().property("name");
            writer.writeVertexProperty(os, p);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readVertexProperty(bais, propertyAttachable -> {
                    assertEquals(p.value(), propertyAttachable.get().value());
                    assertEquals(p.key(), propertyAttachable.get().key());
                    assertEquals(0, IteratorUtils.count(propertyAttachable.get().properties()));
                    called.set(true);
                    return propertyAttachable.get();
                });
            }

            assertTrue(called.get());
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldReadWritePropertyGraphSON() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = writerMaker.apply(graph);
            final Property p = g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("weight");
            writer.writeProperty(os, p);

            final AtomicBoolean called = new AtomicBoolean(false);
            final GraphReader reader = readerMaker.apply(graph);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readProperty(bais, propertyAttachable -> {
                    assertEquals(p.value(), propertyAttachable.get().value());
                    assertEquals(p.key(), propertyAttachable.get().key());
                    called.set(true);
                    return propertyAttachable.get();
                });
            }

            assertTrue(called.get());
        }
    }
}
