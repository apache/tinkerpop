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

import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter;
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
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("x", 1);
        props.put("y", 2);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", props);

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
        assertEquals(2, pdt.getProperties().size());
        assertEquals(1, pdt.getProperties().get("x"));
        assertEquals(2, pdt.getProperties().get("y"));
    }

    @Test
    public void shouldRoundTrip() throws Exception {
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("x", 1);
        props.put("y", 2);
        final ProviderDefinedType original = new ProviderDefinedType("Point", props);

        final ProviderDefinedType result = serializeDeserialize(mapper, original, ProviderDefinedType.class);

        assertEquals(original.getName(), result.getName());
        assertEquals(original.getProperties(), result.getProperties());
    }

    @Test
    public void shouldSerializeNestedPdt() throws Exception {
        final Map<String, Object> innerProps = new LinkedHashMap<>();
        innerProps.put("x", 10);
        innerProps.put("y", 20);
        final ProviderDefinedType inner = new ProviderDefinedType("Point", innerProps);

        final Map<String, Object> outerProps = new LinkedHashMap<>();
        outerProps.put("name", "origin");
        outerProps.put("location", inner);
        final ProviderDefinedType outer = new ProviderDefinedType("NamedPoint", outerProps);

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
        assertTrue(result.getProperties().get("location") instanceof ProviderDefinedType);
        final ProviderDefinedType nestedResult = (ProviderDefinedType) result.getProperties().get("location");
        assertEquals("Point", nestedResult.getName());
        assertEquals(10, nestedResult.getProperties().get("x"));
        assertEquals(20, nestedResult.getProperties().get("y"));
    }

    @Test
    public void shouldHandleNullFieldValues() throws Exception {
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "test");
        props.put("value", null);
        final ProviderDefinedType pdt = new ProviderDefinedType("NullableType", props);

        final ProviderDefinedType result = serializeDeserialize(mapper, pdt, ProviderDefinedType.class);

        assertEquals("NullableType", result.getName());
        assertEquals("test", result.getProperties().get("name"));
        assertNull(result.getProperties().get("value"));
        assertTrue(result.getProperties().containsKey("value"));
    }

    // --- Hydration tests ---

    static class Point {
        final int x;
        final int y;
        Point(int x, int y) { this.x = x; this.y = y; }
    }

    static class PointAdapter implements ProviderDefinedTypeAdapter<Point> {
        @Override public String typeName() { return "Point"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toProperties(Point obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("x", obj.x);
            m.put("y", obj.y);
            return m;
        }
        @Override public Point fromProperties(Map<String, Object> properties) {
            return new Point((int) properties.get("x"), (int) properties.get("y"));
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

        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("x", 3);
        props.put("y", 7);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", props);

        final ProviderDefinedType result = serializeDeserialize(hydratingMapper, pdt, ProviderDefinedType.class);

        assertNotNull(result.getHydrated());
        assertTrue(result.getHydrated() instanceof Point);
        assertEquals(3, ((Point) result.getHydrated()).x);
        assertEquals(7, ((Point) result.getHydrated()).y);
    }

    @Test
    public void shouldNotHydrateWhenNoRegistryConfigured() throws Exception {
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("x", 1);
        props.put("y", 2);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", props);

        final ProviderDefinedType result = serializeDeserialize(mapper, pdt, ProviderDefinedType.class);

        assertNull(result.getHydrated());
        assertEquals("Point", result.getName());
        assertEquals(1, result.getProperties().get("x"));
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

        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("a", 1);
        final ProviderDefinedType pdt = new ProviderDefinedType("Unknown", props);

        final ProviderDefinedType result = serializeDeserialize(hydratingMapper, pdt, ProviderDefinedType.class);

        assertNull(result.getHydrated());
        assertEquals("Unknown", result.getName());
        assertEquals(1, result.getProperties().get("a"));
    }
}
