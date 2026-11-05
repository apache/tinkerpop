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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link GeometrySerializersV4}.
 */
public class GeometryGraphSONTest extends AbstractGraphSONTest {

    private static final String SEATTLE_WKT = "POINT(-122.3321 47.6062)";

    private static final String SEATTLE_JSON =
            "{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"" + SEATTLE_WKT + "\",\"model\":\"GEOGRAPHIC\"}}";

    private static final String SEATTLE_UNTYPED_JSON =
            "{\"wkt\":\"" + SEATTLE_WKT + "\",\"model\":\"GEOGRAPHIC\"}";

    private ObjectMapper mapper;
    private ObjectMapper untypedMapper;

    @Before
    public void setUp() {
        mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create().createMapper();
        untypedMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create().createMapper();
    }

    @Test
    public void shouldWriteGeographicPoint() throws Exception {
        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        assertEquals(SEATTLE_JSON, mapper.writeValueAsString(point));
    }

    @Test
    public void shouldReadGeographicPoint() throws Exception {
        final Geometry point = mapper.readValue(SEATTLE_JSON, Geometry.class);
        assertEquals(Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC), point);
        assertEquals(Model.GEOGRAPHIC, point.model());
    }

    @Test
    public void shouldRoundTripEveryGeometryKind() throws Exception {
        final List<Geometry> geometries = Arrays.asList(
                Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC),
                Geometry.fromWkt("POINT Z(1 2 3)", Model.CARTESIAN),
                Geometry.fromWkt("POINT EMPTY", Model.CARTESIAN),
                Geometry.fromWkt("LINESTRING(0 0, 1 1, 2 0)", Model.CARTESIAN),
                Geometry.fromWkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))", Model.GEOGRAPHIC),
                Geometry.fromWkt("MULTIPOINT((1 2), (3 4))", Model.CARTESIAN),
                Geometry.fromWkt("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))", Model.CARTESIAN),
                Geometry.fromWkt("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))", Model.GEOGRAPHIC),
                Geometry.fromWkt("GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)", Model.CARTESIAN));

        for (final Geometry geometry : geometries) {
            final Geometry read = serializeDeserialize(mapper, geometry, Geometry.class);
            assertEquals(geometry, read);
            assertEquals(geometry.getClass(), read.getClass());
        }
    }

    @Test
    public void shouldRoundTripEveryEmptyKind() throws Exception {
        final String[] values = {
                "POINT EMPTY", "POINT Z EMPTY",
                "LINESTRING EMPTY", "LINESTRING Z EMPTY",
                "POLYGON EMPTY", "POLYGON Z EMPTY",
                "MULTIPOINT EMPTY", "MULTIPOINT Z EMPTY",
                "MULTILINESTRING EMPTY", "MULTILINESTRING Z EMPTY",
                "MULTIPOLYGON EMPTY", "MULTIPOLYGON Z EMPTY",
                "GEOMETRYCOLLECTION EMPTY", "GEOMETRYCOLLECTION Z EMPTY"
        };

        for (final String wkt : values) {
            final Geometry geometry = Geometry.fromWkt(wkt, Model.CARTESIAN);

            assertEquals(wkt, "{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"" + wkt + "\",\"model\":\"CARTESIAN\"}}",
                    mapper.writeValueAsString(geometry));

            final Geometry read = serializeDeserialize(mapper, geometry, Geometry.class);
            assertEquals(wkt, geometry, read);
            assertEquals(wkt, geometry.getClass(), read.getClass());
            assertTrue(wkt, read.isEmpty());
            assertEquals(wkt, geometry.hasZ(), read.hasZ());
        }
    }

    @Test
    public void shouldRoundTripCartesianPointWithSridZero() throws Exception {
        final Geometry point = Geometry.fromWkt("POINT(1 2)", Model.CARTESIAN);
        assertEquals("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1 2)\",\"model\":\"CARTESIAN\"}}",
                mapper.writeValueAsString(point));

        final Geometry read = serializeDeserialize(mapper, point, Geometry.class);
        assertEquals(point, read);
        assertEquals(Model.CARTESIAN, read.model());
        assertEquals(0, read.srid());
    }

    @Test
    public void shouldKeepNegativeZeroOnRoundTrip() throws Exception {
        final Point point = Point.of(-0.0, 0.0);

        assertEquals("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(-0 0)\",\"model\":\"CARTESIAN\"}}",
                mapper.writeValueAsString(point));

        final Point read = (Point) serializeDeserialize(mapper, point, Geometry.class);
        assertEquals(Long.MIN_VALUE, Double.doubleToRawLongBits(read.x()));
        assertEquals(Point.of(0, 0), read);
    }

    @Test
    public void shouldFailToWriteGeometryWithOtherModel() {
        final Geometry point = Geometry.fromWkt("POINT(1 2)", 3857);
        final IOException ioe = assertThrows(IOException.class, () -> mapper.writeValueAsString(point));
        assertTrue(ioe.getMessage(), ioe.getMessage().contains("3857"));
    }

    @Test
    public void shouldFailToReadOtherModel() {
        assertReadFails("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1 2)\",\"model\":\"OTHER\"}}",
                "CARTESIAN or GEOGRAPHIC");
    }

    @Test
    public void shouldFailToReadUnknownModel() {
        assertReadFails("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1 2)\",\"model\":\"MERCATOR\"}}",
                "CARTESIAN or GEOGRAPHIC");
    }

    @Test
    public void shouldFailToReadWithoutWkt() {
        assertReadFails("{\"@type\":\"g:Geometry\",\"@value\":{\"model\":\"CARTESIAN\"}}",
                "string wkt field");
    }

    @Test
    public void shouldFailToReadWithoutModel() {
        assertReadFails("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1 2)\"}}",
                "string model field");
    }

    @Test
    public void shouldReadWktWithSpaceBeforeTheBody() throws Exception {
        final Geometry point = mapper.readValue(
                "{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT (1 2)\",\"model\":\"CARTESIAN\"}}", Geometry.class);
        assertEquals(Geometry.fromWkt("POINT(1 2)", Model.CARTESIAN), point);
    }

    @Test
    public void shouldFailToReadWktWithOneOrdinate() {
        assertReadFails("{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1)\",\"model\":\"CARTESIAN\"}}",
                "Expected number");
    }

    @Test
    public void shouldFailToReadEwktAsWkt() {
        assertReadFails(
                "{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"SRID=4326;POINT(1 2)\",\"model\":\"GEOGRAPHIC\"}}",
                "SRID=");
    }

    @Test
    public void shouldIgnoreUnknownFields() throws Exception {
        final Geometry point = mapper.readValue(
                "{\"@type\":\"g:Geometry\",\"@value\":{\"wkt\":\"POINT(1 2)\",\"model\":\"CARTESIAN\",\"srid\":4326}}",
                Geometry.class);
        assertEquals(Model.CARTESIAN, point.model());
        assertEquals(0, point.srid());
    }

    @Test
    public void shouldRoundTripGeometryAsVertexPropertyValue() throws Exception {
        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        final DetachedVertex vertex = new DetachedVertex(1L, "location", Collections.emptyMap());
        final VertexProperty<Geometry> property = new DetachedVertexProperty<>(
                2L, "position", point, Collections.emptyMap(), vertex);

        final VertexProperty read = serializeDeserialize(mapper, property, VertexProperty.class);
        assertEquals(point, read.value());
    }

    @Test
    public void shouldRoundTripGeometryInTypedMap() throws Exception {
        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("here", point);

        final String json = mapper.writeValueAsString(map);
        assertEquals("{\"@type\":\"g:Map\",\"@value\":[\"here\"," + SEATTLE_JSON + "]}", json);

        final Map read = mapper.readValue(json, Map.class);
        assertEquals(map, read);
        assertEquals(point, read.get("here"));
    }

    @Test
    public void shouldRoundTripGeometryInTypedList() throws Exception {
        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        final List<Geometry> list = Collections.singletonList(point);

        final String json = mapper.writeValueAsString(list);
        assertEquals("{\"@type\":\"g:List\",\"@value\":[" + SEATTLE_JSON + "]}", json);

        final List read = mapper.readValue(json, List.class);
        assertEquals(list, read);
        assertEquals(point, read.get(0));
    }

    @Test
    public void shouldWriteGeometryWithoutTypeInfo() throws Exception {
        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        assertEquals(SEATTLE_UNTYPED_JSON, untypedMapper.writeValueAsString(point));
    }

    @Test
    public void shouldWriteGeometryInMapWithoutTypeInfo() throws Exception {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("here", Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC));
        assertEquals("{\"here\":" + SEATTLE_UNTYPED_JSON + "}", untypedMapper.writeValueAsString(map));
    }

    /**
     * {@link GraphSONXModuleV4} is an addition on top of the core module rather than the home of the geometry
     * serializers, so adding it leaves the output alone.
     */
    @Test
    public void shouldWriteGeometryTheSameWayWithTheExtendedModule() throws Exception {
        final ObjectMapper extendedMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create().createMapper();

        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        assertEquals(SEATTLE_JSON, extendedMapper.writeValueAsString(point));
        assertEquals(mapper.writeValueAsString(point), extendedMapper.writeValueAsString(point));
        assertEquals(point, extendedMapper.readValue(SEATTLE_JSON, Geometry.class));
    }

    /**
     * A mapper built with nothing but a version is what {@code g.io()} and the GraphSON writers use, so a geometry
     * reached through a vertex has to be written as {@code g:Geometry} and not through Jackson's bean handling,
     * which would describe a geometry by its getters as {@code {"empty":false}}.
     */
    @Test
    public void shouldWriteGeometryInVertexWithDefaultMapper() throws Exception {
        final ObjectMapper defaultMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .create().createMapper();

        final Geometry point = Geometry.fromWkt(SEATTLE_WKT, Model.GEOGRAPHIC);
        final Vertex vertex = new DetachedVertex(1L, "location", Collections.singletonList(
                DetachedVertexProperty.build().setId(2L).setLabel("position").setValue(point).create()));

        final String json = defaultMapper.writeValueAsString(vertex);
        assertEquals("{\"@type\":\"g:Vertex\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},"
                        + "\"label\":[\"location\"],\"properties\":{\"position\":[{\"@type\":\"g:VertexProperty\","
                        + "\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":2},\"value\":" + SEATTLE_JSON
                        + ",\"label\":[\"position\"]}}]}}}",
                json);
        assertFalse(json, json.contains("empty"));
    }

    /**
     * Asserts that the read fails and that the message names what the deserializer refused, so that
     * removing the check in question fails the test rather than leaving it to pass on a later error.
     */
    private void assertReadFails(final String json, final String expectedInMessage) {
        final IOException ioe = assertThrows(IOException.class, () -> mapper.readValue(json, Geometry.class));
        assertTrue(ioe.getMessage(), ioe.getMessage().contains(expectedInMessage));
    }
}
