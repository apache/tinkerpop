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

import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDTAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitiveProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PdtGraphSONSerializersV4}.
 */
public class PdtGraphSONSerializersV4Test extends AbstractGraphSONTest {

    private ObjectMapper mapper;
    private ObjectMapper plainMapper;

    @Before
    public void setUp() {
        mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create().createMapper();
        plainMapper = new ObjectMapper();
    }

    @Test
    public void shouldSerializeSimplePdt() throws Exception {
        final Map<String, Object> pdtFields = new LinkedHashMap<>();
        pdtFields.put("x", 1);
        pdtFields.put("y", 2);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", pdtFields);

        final String json = mapper.writeValueAsString(pdt);
        final JsonNode node = plainMapper.readTree(json);

        assertEquals("g:CompositePdt", node.get("@type").asText());
        final JsonNode value = node.get("@value");
        assertEquals("Point", value.get("type").asText());

        final JsonNode fields = value.get("fields");
        assertEquals("g:Int32", fields.get("x").get("@type").asText());
        assertEquals(1, fields.get("x").get("@value").asInt());
        assertEquals("g:Int32", fields.get("y").get("@type").asText());
        assertEquals(2, fields.get("y").get("@value").asInt());
    }

    @Test
    public void shouldDeserializeValidJson() throws Exception {
        final String json = "{\"@type\":\"g:CompositePdt\",\"@value\":{\"type\":\"Point\",\"fields\":{\"x\":{\"@type\":\"g:Int32\",\"@value\":1},\"y\":{\"@type\":\"g:Int32\",\"@value\":2}}}}";
        final ProviderDefinedType pdt = mapper.readValue(json, ProviderDefinedType.class);

        assertEquals("Point", pdt.getName());
        assertEquals(2, pdt.getFields().size());
        assertEquals(1, pdt.getFields().get("x"));
        assertEquals(2, pdt.getFields().get("y"));
    }

    @Test
    public void shouldRoundTrip() throws Exception {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final ProviderDefinedType original = new ProviderDefinedType("Point", fields);

        final ProviderDefinedType result = serializeDeserialize(mapper, original, ProviderDefinedType.class);

        assertEquals(original.getName(), result.getName());
        assertEquals(original.getFields(), result.getFields());
    }

    @Test
    public void shouldSerializeNestedPdt() throws Exception {
        final Map<String, Object> innerFields = new LinkedHashMap<>();
        innerFields.put("x", 10);
        innerFields.put("y", 20);
        final ProviderDefinedType inner = new ProviderDefinedType("Point", innerFields);

        final Map<String, Object> outerFields = new LinkedHashMap<>();
        outerFields.put("name", "origin");
        outerFields.put("location", inner);
        final ProviderDefinedType outer = new ProviderDefinedType("NamedPoint", outerFields);

        final String json = mapper.writeValueAsString(outer);
        final JsonNode node = plainMapper.readTree(json);

        assertEquals("g:CompositePdt", node.get("@type").asText());
        final JsonNode fields = node.get("@value").get("fields");
        final JsonNode locationNode = fields.get("location");
        assertEquals("g:CompositePdt", locationNode.get("@type").asText());
        assertEquals("Point", locationNode.get("@value").get("type").asText());

        // round-trip nested
        final ProviderDefinedType result = serializeDeserialize(mapper, outer, ProviderDefinedType.class);
        assertEquals("NamedPoint", result.getName());
        assertTrue(result.getFields().get("location") instanceof ProviderDefinedType);
        final ProviderDefinedType nestedResult = (ProviderDefinedType) result.getFields().get("location");
        assertEquals("Point", nestedResult.getName());
        assertEquals(10, nestedResult.getFields().get("x"));
        assertEquals(20, nestedResult.getFields().get("y"));
    }

    @Test
    public void shouldHandleNullFieldValues() throws Exception {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("name", "test");
        fields.put("value", null);
        final ProviderDefinedType pdt = new ProviderDefinedType("NullableType", fields);

        final ProviderDefinedType result = serializeDeserialize(mapper, pdt, ProviderDefinedType.class);

        assertEquals("NullableType", result.getName());
        assertEquals("test", result.getFields().get("name"));
        assertNull(result.getFields().get("value"));
        assertTrue(result.getFields().containsKey("value"));
    }

    // --- Hydration tests ---

    static class Point {
        final int x;
        final int y;
        Point(int x, int y) { this.x = x; this.y = y; }
    }

    static class PointAdapter implements CompositePDTAdapter<Point> {
        @Override public String typeName() { return "Point"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toFields(Point obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("x", obj.x);
            m.put("y", obj.y);
            return m;
        }
        @Override public Point fromFields(Map<String, Object> fields) {
            return new Point((int) fields.get("x"), (int) fields.get("y"));
        }
    }

    @Test
    public void shouldHydrateWhenRegistryConfigured() throws Exception {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());

        final ObjectMapper hydratingMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .pdtRegistry(registry)
                .create().createMapper();

        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 3);
        fields.put("y", 7);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", fields);

        final ProviderDefinedType result = serializeDeserialize(hydratingMapper, pdt, ProviderDefinedType.class);

        assertNotNull(result.getHydrated());
        assertTrue(result.getHydrated() instanceof Point);
        assertEquals(3, ((Point) result.getHydrated()).x);
        assertEquals(7, ((Point) result.getHydrated()).y);
    }

    @Test
    public void shouldNotHydrateWhenNoRegistryConfigured() throws Exception {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", fields);

        final ProviderDefinedType result = serializeDeserialize(mapper, pdt, ProviderDefinedType.class);

        assertNull(result.getHydrated());
        assertEquals("Point", result.getName());
        assertEquals(1, result.getFields().get("x"));
    }

    @Test
    public void shouldDehydrateRegisteredButUnannotatedTypeViaAdapterOnWritePath() throws Exception {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());

        final ObjectMapper adapterMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .pdtRegistry(registry)
                .create().createMapper();

        final Point original = new Point(5, 9);
        final ProviderDefinedType result = serializeDeserialize(adapterMapper, original, ProviderDefinedType.class);

        assertNotNull(result.getHydrated());
        assertTrue(result.getHydrated() instanceof Point);
        assertEquals(5, ((Point) result.getHydrated()).x);
        assertEquals(9, ((Point) result.getHydrated()).y);
    }

    @Test
    public void shouldReturnRawPdtWhenTypeNotRegistered() throws Exception {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        // No adapter registered for "Unknown"

        final ObjectMapper hydratingMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .pdtRegistry(registry)
                .create().createMapper();

        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("a", 1);
        final ProviderDefinedType pdt = new ProviderDefinedType("Unknown", fields);

        final ProviderDefinedType result = serializeDeserialize(hydratingMapper, pdt, ProviderDefinedType.class);

        assertNull(result.getHydrated());
        assertEquals("Unknown", result.getName());
        assertEquals(1, result.getFields().get("a"));
    }

    // --- PrimitivePDT tests ---

    @Test
    public void shouldSerializePrimitivePdt() throws Exception {
        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Duration", "PT5M");

        final String json = mapper.writeValueAsString(pdt);
        final JsonNode node = plainMapper.readTree(json);

        assertEquals("g:PrimitivePdt", node.get("@type").asText());
        final JsonNode value = node.get("@value");
        assertEquals("Duration", value.get("type").asText());
        // value must be an untyped JSON string (no @type/@value wrapping)
        assertTrue(value.get("value").isTextual());
        assertEquals("PT5M", value.get("value").asText());
    }

    @Test
    public void shouldDeserializePrimitivePdt() throws Exception {
        final String json = "{\"@type\":\"g:PrimitivePdt\",\"@value\":{\"type\":\"Duration\",\"value\":\"PT5M\"}}";
        final PrimitiveProviderDefinedType pdt = mapper.readValue(json, PrimitiveProviderDefinedType.class);

        assertEquals("Duration", pdt.getName());
        assertEquals("PT5M", pdt.getValue());
    }

    @Test
    public void shouldRoundTripPrimitivePdt() throws Exception {
        final PrimitiveProviderDefinedType original = new PrimitiveProviderDefinedType("Duration", "PT5M");
        final PrimitiveProviderDefinedType result = serializeDeserialize(mapper, original, PrimitiveProviderDefinedType.class);

        assertEquals(original.getName(), result.getName());
        assertEquals(original.getValue(), result.getValue());
    }

    @Test
    public void shouldHydratePrimitivePdtWhenRegistryConfigured() throws Exception {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new DurationAdapter());

        final ObjectMapper hydratingMapper = GraphSONMapper.build()
                .version(GraphSONVersion.V4_0)
                .addCustomModule(GraphSONXModuleV4.build())
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .pdtRegistry(registry)
                .create().createMapper();

        final PrimitiveProviderDefinedType pdt = new PrimitiveProviderDefinedType("Duration", "PT10S");
        final PrimitiveProviderDefinedType result = serializeDeserialize(hydratingMapper, pdt, PrimitiveProviderDefinedType.class);

        assertNotNull(result.getHydrated());
        assertTrue(result.getHydrated() instanceof MyDuration);
        assertEquals(10, ((MyDuration) result.getHydrated()).seconds);
    }

    @Test
    public void shouldNestPrimitivePdtInsideCompositePdt() throws Exception {
        final PrimitiveProviderDefinedType inner = new PrimitiveProviderDefinedType("Duration", "PT1H");
        final Map<String, Object> outerFields = new LinkedHashMap<>();
        outerFields.put("name", "timeout");
        outerFields.put("dur", inner);
        final ProviderDefinedType outer = new ProviderDefinedType("Config", outerFields);

        final String json = mapper.writeValueAsString(outer);
        final JsonNode node = plainMapper.readTree(json);

        assertEquals("g:CompositePdt", node.get("@type").asText());
        final JsonNode durNode = node.get("@value").get("fields").get("dur");
        assertEquals("g:PrimitivePdt", durNode.get("@type").asText());
        assertEquals("Duration", durNode.get("@value").get("type").asText());
        assertEquals("PT1H", durNode.get("@value").get("value").asText());

        // round-trip
        final ProviderDefinedType result = serializeDeserialize(mapper, outer, ProviderDefinedType.class);
        assertEquals("Config", result.getName());
        assertTrue(result.getFields().get("dur") instanceof PrimitiveProviderDefinedType);
        final PrimitiveProviderDefinedType nestedResult = (PrimitiveProviderDefinedType) result.getFields().get("dur");
        assertEquals("Duration", nestedResult.getName());
        assertEquals("PT1H", nestedResult.getValue());
    }

    // helper types for primitive PDT hydration tests

    static class MyDuration {
        final int seconds;
        MyDuration(int seconds) { this.seconds = seconds; }
    }

    static class DurationAdapter implements PrimitivePDTAdapter<MyDuration> {
        @Override public String typeName() { return "Duration"; }
        @Override public Class<MyDuration> targetClass() { return MyDuration.class; }
        @Override public String toValue(MyDuration obj) { return "PT" + obj.seconds + "S"; }
        @Override public MyDuration fromValue(String value) {
            // parse PTnS
            final String stripped = value.replaceAll("[PTS]", "").replace("H", "").replace("M", "");
            return new MyDuration(Integer.parseInt(stripped));
        }
    }
}
