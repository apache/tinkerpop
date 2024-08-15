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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GValueTest {

    @Test
    public void shouldCreateGValueFromValue() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(123, gValue.get().intValue());
        assertEquals(GType.INTEGER, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromNameAndValue() {
        final GValue<Integer> gValue = GValue.of("varName", 123);
        assertEquals(123, gValue.get().intValue());
        assertEquals(GType.INTEGER, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromString() {
        final GValue<String> gValue = GValue.ofString("test");
        assertEquals("test", gValue.get());
        assertEquals(GType.STRING, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromStringWithName() {
        final GValue<String> gValue = GValue.ofString("varName", "test");
        assertEquals("test", gValue.get());
        assertEquals(GType.STRING, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromInteger() {
        final GValue<Integer> gValue = GValue.ofInteger(123);
        assertEquals(123, gValue.get().intValue());
        assertEquals(GType.INTEGER, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromIntegerWithName() {
        final GValue<Integer> gValue = GValue.ofInteger("varName", 123);
        assertEquals(123, gValue.get().intValue());
        assertEquals(GType.INTEGER, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBoolean() {
        final GValue<Boolean> gValue = GValue.ofBoolean(true);
        assertEquals(true, gValue.get());
        assertEquals(GType.BOOLEAN, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBooleanWithName() {
        final GValue<Boolean> gValue = GValue.ofBoolean("varName", true);
        assertEquals(true, gValue.get());
        assertEquals(GType.BOOLEAN, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromDouble() {
        final GValue<Double> gValue = GValue.ofDouble(123.45);
        assertEquals(123.45, gValue.get(), 0.0);
        assertEquals(GType.DOUBLE, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromDoubleWithName() {
        final GValue<Double> gValue = GValue.ofDouble("varName", 123.45);
        assertEquals(123.45, gValue.get(), 0.0);
        assertEquals(GType.DOUBLE, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBigInteger() {
        final GValue<BigInteger> gValue = GValue.ofBigInteger(BigInteger.ONE);
        assertEquals(BigInteger.ONE, gValue.get());
        assertEquals(GType.BIG_INTEGER, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBigIntegerWithName() {
        final GValue<BigInteger> gValue = GValue.ofBigInteger("varName", BigInteger.ONE);
        assertEquals(BigInteger.ONE, gValue.get());
        assertEquals(GType.BIG_INTEGER, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBigDecimal() {
        final GValue<BigDecimal> gValue = GValue.ofBigDecimal(BigDecimal.ONE);
        assertEquals(BigDecimal.ONE, gValue.get());
        assertEquals(GType.BIG_DECIMAL, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBigDecimalWithName() {
        final GValue<BigDecimal> gValue = GValue.ofBigDecimal("varName", BigDecimal.ONE);
        assertEquals(BigDecimal.ONE, gValue.get());
        assertEquals(GType.BIG_DECIMAL, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromLong() {
        final GValue<Long> gValue = GValue.ofLong(123L);
        assertEquals(123L, gValue.get().longValue());
        assertEquals(GType.LONG, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromLongWithName() {
        final GValue<Long> gValue = GValue.ofLong("varName", 123L);
        assertEquals(123L, gValue.get().longValue());
        assertEquals(GType.LONG, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromMap() {
        final Map<String, String> map = new HashMap<String,String>() {{
            put("key", "value");
        }};
        final GValue<Map> gValue = GValue.ofMap(map);
        assertEquals(map, gValue.get());
        assertEquals(GType.MAP, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromMapWithName() {
        final Map<String, String> map = new HashMap<String,String>() {{
            put("key", "value");
        }};
        final GValue<Map> gValue = GValue.ofMap("varName", map);
        assertEquals(map, gValue.get());
        assertEquals(GType.MAP, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromList() {
        final List<String> list = Arrays.asList("value1", "value2");
        final GValue<List<String>> gValue = GValue.ofList(list);
        assertEquals(list, gValue.get());
        assertEquals(GType.LIST, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromListWithName() {
        final List<String> list = Arrays.asList("value1", "value2");
        final GValue<List<String>> gValue = GValue.ofList("varName", list);
        assertEquals(list, gValue.get());
        assertEquals(GType.LIST, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromSet() {
        final Set<String> set = new HashSet<>(Arrays.asList("value1", "value2"));
        final GValue<Set> gValue = GValue.ofSet(set);
        assertEquals(set, gValue.get());
        assertEquals(GType.SET, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromSetWithName() {
        final Set<String> set = new HashSet<>(Arrays.asList("value1", "value2"));
        final GValue<Set> gValue = GValue.ofSet("varName", set);
        assertEquals(set, gValue.get());
        assertEquals(GType.SET, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromVertex() {
        final Vertex vertex = mock(Vertex.class);
        final GValue<Vertex> gValue = GValue.ofVertex(vertex);
        assertEquals(vertex, gValue.get());
        assertEquals(GType.VERTEX, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromVertexWithName() {
        final Vertex vertex = mock(Vertex.class);
        final GValue<Vertex> gValue = GValue.ofVertex("varName", vertex);
        assertEquals(vertex, gValue.get());
        assertEquals(GType.VERTEX, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromEdge() {
        final Edge edge = mock(Edge.class);
        final GValue<Edge> gValue = GValue.ofEdge(edge);
        assertEquals(edge, gValue.get());
        assertEquals(GType.EDGE, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromEdgeWithName() {
        final Edge edge = mock(Edge.class);
        final GValue<Edge> gValue = GValue.ofEdge("varName", edge);
        assertEquals(edge, gValue.get());
        assertEquals(GType.EDGE, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromPath() {
        final Path path = mock(Path.class);
        final GValue<Path> gValue = GValue.ofPath(path);
        assertEquals(path, gValue.get());
        assertEquals(GType.PATH, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromPathWithName() {
        final Path path = mock(Path.class);
        final GValue<Path> gValue = GValue.ofPath("varName", path);
        assertEquals(path, gValue.get());
        assertEquals(GType.PATH, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromProperty() {
        final Property property = mock(Property.class);
        final GValue<Property> gValue = GValue.ofProperty(property);
        assertEquals(property, gValue.get());
        assertEquals(GType.PROPERTY, gValue.getType());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromPropertyWithName() {
        final Property property = mock(Property.class);
        final GValue<Property> gValue = GValue.ofProperty("varName", property);
        assertEquals(property, gValue.get());
        assertEquals(GType.PROPERTY, gValue.getType());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }
}