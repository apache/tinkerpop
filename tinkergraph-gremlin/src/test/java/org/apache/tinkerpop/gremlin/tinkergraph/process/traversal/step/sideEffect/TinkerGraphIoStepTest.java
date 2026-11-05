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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.io.util.CustomId;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * It was hard to test the {@link IO#registry} configuration as a generic test. Opted to test it as a bit of a
 * standalone test with TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphIoStepTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    @Test
    public void shouldWriteReadWithCustomIoRegistryGryo() throws Exception {
        final UUID uuid = UUID.randomUUID();
        g.addV("person").property("name","stephen").property("custom", new CustomId("a", uuid)).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldWriteReadWithCustomIoRegistryGryo", ".kryo");
        g.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.class.getName()).write().iterate();

        final Graph emptyGraph = TinkerGraph.open();
        final GraphTraversalSource emptyG = emptyGraph.traversal();

        try {
            emptyG.io(file.getAbsolutePath()).read().iterate();
            fail("Can't read without a registry");
        } catch (Exception ignored) {
            // do nothing
        }

        emptyG.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.instance()).read().iterate();

        assertEquals(1, emptyG.V().has("custom", new CustomId("a", uuid)).count().next().intValue());
    }

    @Test
    public void shouldWriteReadWithCustomIoRegistryGraphSON() throws Exception {
        final UUID uuid = UUID.randomUUID();
        g.addV("person").property("name","stephen").property("custom", new CustomId("a", uuid)).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldWriteReadWithCustomIoRegistryGraphSON", ".json");
        g.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.class.getName()).write().iterate();

        final Graph emptyGraph = TinkerGraph.open();
        final GraphTraversalSource emptyG = emptyGraph.traversal();

        try {
            emptyG.io(file.getAbsolutePath()).read().iterate();
            fail("Can't read without a registry");
        } catch (Exception ignored) {
            // do nothing
        }

        emptyG.io(file.getAbsolutePath()).with(IO.registry, CustomId.CustomIdIoRegistry.instance()).read().iterate();

        assertEquals(1, emptyG.V().has("custom", new CustomId("a", uuid)).count().next().intValue());
    }

    @Test
    public void shouldWriteReadGeometryGryo() throws Exception {
        final Geometry point = Geometry.fromWkt("POINT(-122.3321 47.6062)", Model.GEOGRAPHIC);
        final Geometry collection = Geometry.fromWkt("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))", Model.CARTESIAN);
        final Geometry projected = Geometry.fromWkt("POINT(560000 5280000)", 32618);

        g.addV("place").property("name", "seattle").
                property("location", point).
                property("area", collection).
                property("projected", projected).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldWriteReadGeometryGryo", ".kryo");
        g.io(file.getAbsolutePath()).with(IO.writer, IO.gryo).write().iterate();

        final GraphTraversalSource emptyG = TinkerGraph.open().traversal();
        emptyG.io(file.getAbsolutePath()).with(IO.reader, IO.gryo).read().iterate();

        assertEquals(point, emptyG.V().has("name", "seattle").values("location").next());
        assertEquals(collection, emptyG.V().has("name", "seattle").values("area").next());

        final Geometry readProjected = (Geometry) emptyG.V().has("name", "seattle").values("projected").next();
        assertEquals(projected, readProjected);
        assertEquals(32618, readProjected.srid());
        assertEquals(Model.OTHER, readProjected.model());
    }

    @Test
    public void shouldWriteReadGeometryGraphSON() throws Exception {
        final Geometry point = Geometry.fromWkt("POINT(-122.3321 47.6062)", Model.GEOGRAPHIC);
        final Geometry collection = Geometry.fromWkt("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(0 0, 1 1))", Model.CARTESIAN);

        g.addV("place").property("name", "seattle").
                property("location", point).
                property("area", collection).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldWriteReadGeometryGraphSON", ".json");
        g.io(file.getAbsolutePath()).with(IO.writer, IO.graphson).write().iterate();

        final GraphTraversalSource emptyG = TinkerGraph.open().traversal();
        emptyG.io(file.getAbsolutePath()).with(IO.reader, IO.graphson).read().iterate();

        assertEquals(point, emptyG.V().has("name", "seattle").values("location").next());
        assertEquals(collection, emptyG.V().has("name", "seattle").values("area").next());
    }

    /**
     * GraphSON names a geometry's coordinate system as {@code CARTESIAN} or {@code GEOGRAPHIC} and has no way to
     * carry any other SRID, so a write that would lose one is refused rather than silently dropping it.
     */
    @Test
    public void shouldFailToWriteGeometryWithOtherModelGraphSON() throws Exception {
        g.addV("place").property("name", "zone18n").
                property("projected", Geometry.fromWkt("POINT(560000 5280000)", 32618)).iterate();

        final File file = TestHelper.generateTempFile(TinkerGraphIoStepTest.class, "shouldFailToWriteGeometryWithOtherModelGraphSON", ".json");
        try {
            g.io(file.getAbsolutePath()).with(IO.writer, IO.graphson).write().iterate();
            fail("Can't write a geometry whose SRID has no GraphSON model");
        } catch (Exception ex) {
            final String message = ExceptionUtils.getRootCauseMessage(ex);
            assertThat(message, containsString("32618"));
        }
    }
}
