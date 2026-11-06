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
package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

/**
 * Confirms that a {@link StarGraph}-wrapped multi-label {@link Vertex} round-trips through GraphSON V4 without
 * losing any of its labels, and that single-label vertices continue to round-trip through V1/V2/V3 as before.
 */
public class StarGraphGraphSONV4MultiLabelTest {

    @Test
    public void shouldRoundTripMultiLabelVertexThroughGraphSONV4() throws Exception {
        final StarGraph starGraph = StarGraph.open();
        final Vertex original = starGraph.addVertex(T.label, "animal");
        ((StarGraph.StarVertex) original).setLabels(new LinkedHashSet<>(Arrays.asList("animal", "bird", "aquatic", "endangered")));

        final GraphSONWriter writer = GraphSONWriter.build().
                mapper(GraphSONMapper.build().version(GraphSONVersion.V4_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).
                create();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer.writeVertex(baos, original, Direction.BOTH);

        final GraphSONReader reader = GraphSONReader.build().
                mapper(GraphSONMapper.build().version(GraphSONVersion.V4_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).
                create();
        final Vertex roundTripped = reader.readVertex(new ByteArrayInputStream(baos.toByteArray()), Attachable::get);

        assertThat(roundTripped.labels(), is(new LinkedHashSet<>(Arrays.asList("animal", "bird", "aquatic", "endangered"))));
    }

    @Test
    public void shouldRoundTripSingleLabelVertexThroughGraphSONV3() throws Exception {
        final StarGraph starGraph = StarGraph.open();
        final Vertex original = starGraph.addVertex(T.label, "person");

        final GraphSONWriter writer = GraphSONWriter.build().
                mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).
                create();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer.writeVertex(baos, original, Direction.BOTH);

        final GraphSONReader reader = GraphSONReader.build().
                mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).
                create();
        final Vertex roundTripped = reader.readVertex(new ByteArrayInputStream(baos.toByteArray()), Attachable::get);

        assertThat(roundTripped.label(), is("person"));
        assertThat(roundTripped.labels(), contains("person"));
    }
}
