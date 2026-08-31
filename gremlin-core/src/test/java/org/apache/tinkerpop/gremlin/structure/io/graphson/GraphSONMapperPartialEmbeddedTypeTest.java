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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import com.example.gadget.GraphSONTestGadgets.SamplePojo;
import com.example.gadget.GraphSONTestGadgets.SamplePojoSubclass;
import com.example.gadget.GraphSONTestGadgets.StaticInitCanary;
import com.example.gadget.GraphSONTestGadgets.StaticInitCanaryElement;
import com.example.gadget.GraphSONTestGadgets.StaticInitCanaryEnum;
import com.example.gadget.GraphSONTestGadgets.StaticInitCanaryValue;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.exc.InvalidFormatException;
import org.apache.tinkerpop.shaded.jackson.databind.exc.InvalidTypeIdException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Currency;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Tests automatic typed serialization/deserialization for GraphSON 2.0+.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONMapperPartialEmbeddedTypeTest extends AbstractGraphSONTest {

    /**
     * The key the value under test is held under in the GraphSON 1.0 cases that nest it in a {@code Map}.
     */
    private static final String MAP_VALUE_KEY = "v";

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0)
                        .addCustomModule(GraphSONXModuleV2.build())
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v3", GraphSONMapper.build().version(GraphSONVersion.V3_0)
                        .addCustomModule(GraphSONXModuleV3.build())
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()}
        });
    }

    @Parameterized.Parameter(0)
    public String version;

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;

    @Test
    public void elementOrderShouldNotMatter() throws Exception {
        final String bytecodeJSONFail1 = "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@type\":\"g:Int32\",\"@value\":-4294967295}]]}}";
        final String bytecodeJSONFail2 = "{\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@value\":-4294967295,\"@type\":\"g:Int32\"}]]},\"@type\":\"g:Bytecode\"}";

        // first validate the failures of TINKERPOP-1738 - prior to the jackson fix on 2.9.4 one of these would have
        // passed based on the ordering of the properties
        try {
            mapper.readValue(bytecodeJSONFail1, Bytecode.class);
            fail("Should have thrown an error because 'bigint1value' is not an int32");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(JsonMappingException.class));
        }

        try {
            mapper.readValue(bytecodeJSONFail2, Bytecode.class);
            fail("Should have thrown an error because 'bigint1value' is not an int32");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(JsonMappingException.class));
        }

        // now do a legit parsing based on order
        final String bytecodeJSON1 = "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@type\":\"g:Int64\",\"@value\":-4294967295}]]}}";
        final String bytecodeJSON2 = "{\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@value\":-4294967295,\"@type\":\"g:Int64\"}]]},\"@type\":\"g:Bytecode\"}";

        final Bytecode bytecode1 = mapper.readValue(bytecodeJSON1, Bytecode.class);
        final Bytecode bytecode2 = mapper.readValue(bytecodeJSON2, Bytecode.class);
        assertEquals(bytecode1, bytecode2);
    }

    @Test
    public void shouldSerializeDeserializeNestedCollectionsAndMapAndTypedValuesCorrectly() throws Exception {
        // Trying to fail the TypeDeserializer type detection
        final UUID uuid = UUID.randomUUID();
        final List<Object> myList = new ArrayList<>();

        final List<Object> myList2 = new ArrayList<>();
        myList2.add(UUID.randomUUID());
        myList2.add(33L);
        myList2.add(84);
        final Map<String,Object> map2 = new HashMap<>();
        map2.put("eheh", UUID.randomUUID());
        map2.put("normal", "normal");
        myList2.add(map2);

        final Map<String, Object> map1 = new HashMap<>();
        map1.put("hello", "world");
        map1.put("test", uuid);
        map1.put("hehe", myList2);
        myList.add(map1);

        myList.add("kjkj");
        myList.add(UUID.randomUUID());
        assertEquals(myList, serializeDeserializeAuto(mapper, myList));

        // no "@value" property
        String s = "{\""+GraphSONTokens.VALUETYPE+"\":\"" + GraphSONTokens.GREMLIN_TYPE_NAMESPACE + ":UUID\", \"test\":2}";
        Map<String,Object> map = new LinkedHashMap<>();
        map.put(GraphSONTokens.VALUETYPE, GraphSONTokens.GREMLIN_TYPE_NAMESPACE + ":UUID");
        map.put("test", 2);
        Object res = mapper.readValue(s, Object.class);
        assertEquals(map, res);

        // "@value" and "@type" property reversed
        s = "{\""+GraphSONTokens.VALUEPROP+"\":2, \"" + GraphSONTokens.VALUETYPE + "\":\"" + GraphSONTokens.GREMLIN_TYPE_NAMESPACE + ":Int64\"}";
        res = mapper.readValue(s, Object.class);
        assertEquals(res, 2L);
        assertEquals(res.getClass(), Long.class);

        // no "@type" property.
        s = "{\""+GraphSONTokens.VALUEPROP + "\":2, \"id\":2}";
        map = new LinkedHashMap<>();
        map.put(GraphSONTokens.VALUEPROP, 2);
        map.put("id", 2);
        res = mapper.readValue(s, Object.class);
        assertEquals(res, map);
    }

    @Test
    public void shouldFailIfMoreThanTwoPropertiesInATypePattern() {
        String s = "{\"" + GraphSONTokens.VALUEPROP + "\":2, \"" + GraphSONTokens.VALUETYPE + "\":\""+GraphSONTokens.GREMLIN_TYPE_NAMESPACE +":Int64\", \"hello\": \"world\"}";
        try {
            mapper.readValue(s, Object.class);
            fail("Should have failed deserializing because there's more than properties in the type.");
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("Detected the type pattern in the JSON payload but the map containing the types and values contains other fields. This is not allowed by the deserializer."));
        }
        s = "{\"" + GraphSONTokens.VALUETYPE + "\":\""+GraphSONTokens.GREMLIN_TYPE_NAMESPACE +":Int64\",\"" + GraphSONTokens.VALUEPROP + "\":2, \"hello\": \"world\"}";
        try {
            mapper.readValue(s, Object.class);
            fail("Should have failed deserializing because there's more than properties in the type.");
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("Detected the type pattern in the JSON payload but the map containing the types and values contains other fields. This is not allowed by the deserializer."));
        }
    }

    @Test
    public void shouldFailIfTypeSpecifiedIsNotSameTypeInPayload() {
        final ZoneOffset o = ZonedDateTime.now().getOffset();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            mapper.writeValue(stream, o);
            final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
            // What has been serialized is a ZoneOffset with the type, but the user explicitly requires another type.
            mapper.readValue(inputStream, Instant.class);
            fail("Should have failed decoding the value");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Could not deserialize the JSON value as required. Nested exception: java.lang.InstantiationException: Cannot deserialize the value with the detected type contained in the JSON ('" + GraphSONTokens.GREMLINX_TYPE_NAMESPACE + ":ZoneOffset') to the type specified in parameter to the object mapper (class java.time.Instant). Those types are incompatible."));
        }
    }

    @Test
    public void shouldHandleRawPOJOs() throws Exception {
        final FunObject funObject = new FunObject();
        funObject.setVal("test");
        assertEquals(funObject.toString(), serializeDeserialize(mapper, funObject, FunObject.class).toString());
        assertEquals(funObject.getClass(), serializeDeserialize(mapper, funObject, FunObject.class).getClass());
    }

    @Test
    public void shouldHandleMapWithTypesUsingEmbedTypeSettingV2() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100L, read.get("test"));
    }

    @Test
    public void shouldNotHandleMapWithTypesUsingEmbedTypeSettingV2() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100, read.get("test"));
    }

    @Test
    public void shouldHandleMapWithTypesUsingEmbedTypeSettingV1() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100L, read.get("test"));
    }

    @Test
    public void shouldRejectNetworkPackageTypeWithEmbedTypeSettingV1() {
        // a name resolves only by being listed, so java.net.URL does not, even though the sibling
        // java.net.InetAddress that GraphSON 2.0/3.0 register does
        assertDeniedByTypeValidator(v1Typed(),
                "{\"@class\":\"java.util.HashMap\",\"v\":{\"@class\":\"java.net.URL\",\"u\":\"http://example.com\"}}");
    }

    @Test
    public void shouldRoundTripArraysWithEmbedTypeSettingV1() throws Exception {
        // an array type id is decided by its component name, and a nested array carries one per level. A primitive
        // component ("[B") names no class, while an object component ("[Ljava.lang.String;") names one.
        final ObjectMapper mapper = v1Typed();
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) roundTripInMap(mapper, new byte[]{1, 2, 3}));
        assertArrayEquals(new Boolean[]{true, false},
                (Boolean[]) roundTripInMap(mapper, new Boolean[]{true, false}));
        assertArrayEquals(new String[]{"a", "b"}, (String[]) roundTripInMap(mapper, new String[]{"a", "b"}));
        assertThat(Arrays.deepEquals(new String[][]{{"a"}, {"b"}},
                (String[][]) roundTripInMap(mapper, new String[][]{{"a"}, {"b"}})), is(true));
    }

    @Test
    public void shouldRoundTripSqlAndUtilValueTypesWithEmbedTypeSettingV1() throws Exception {
        // java.sql.Time is written as its toString, which formats and parses back in the default time zone, so
        // valueOf of a fixed literal round-trips anywhere
        final ObjectMapper mapper = v1Typed();
        assertRoundTripsInMap(mapper, Arrays.asList(
                new java.sql.Timestamp(0L),
                java.sql.Time.valueOf("12:34:56"),
                Locale.US,
                Currency.getInstance("USD")));

        // java.util.ArrayDeque does not define value equality, so it is compared element-wise
        final Object read = roundTripInMap(mapper, new ArrayDeque<>(Arrays.asList("a", "b")));
        assertThat(read, instanceOf(ArrayDeque.class));
        assertEquals(Arrays.asList("a", "b"), new ArrayList<>((ArrayDeque<?>) read));
    }

    @Test
    public void shouldRejectArrayOfDisallowedComponentWithEmbedTypeSettingV1() {
        // an array type id is decided by its component name, so an unlisted component does not resolve
        assertDeniedByTypeValidator(v1Typed(),
                "{\"@class\":\"java.util.HashMap\",\"v\":[\"[Ljava.io.File;\",[\"/tmp/x\"]]}");
    }

    @Test
    public void shouldAllowConfiguredTypeIdNameWithEmbedTypeSettingV1() throws Exception {
        final String json = "{\"@class\":\"java.util.HashMap\",\"p\":{\"@class\":\"com.example.gadget.GraphSONTestGadgets$SamplePojo\",\"x\":42}}";

        // not among the allowed names by default
        assertDeniedByTypeValidator(v1Typed(), json);

        final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName("com.example.gadget.GraphSONTestGadgets$SamplePojo").create().createMapper();
        final Map read = mapper.readValue(json, HashMap.class);
        assertEquals(new SamplePojo(42), read.get("p"));
    }

    @Test
    public void shouldAllowConfiguredTypeIdNameInsideCollectionWithEmbedTypeSettingV1() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName(SamplePojo.class.getName())
                .create().createMapper();

        final Object read = roundTripInMap(mapper, Collections.singletonList(new SamplePojo(42)));
        assertEquals(Collections.singletonList(new SamplePojo(42)), read);
        assertEquals(SamplePojo.class, ((List<?>) read).get(0).getClass());
    }

    @Test
    public void shouldAllowArrayOfConfiguredTypeIdNameButRejectSubclassWithEmbedTypeSettingV1() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName(SamplePojo.class.getName())
                .create().createMapper();

        assertArrayEquals(new SamplePojo[]{new SamplePojo(42)},
                (SamplePojo[]) roundTripInMap(mapper, new SamplePojo[]{new SamplePojo(42)}));

        final String subclassTypeId = SamplePojoSubclass.class.getName();
        assertTypeIdDeniedByTypeValidator(mapper,
                "{\"@class\":\"java.util.HashMap\",\"p\":{\"@class\":\"" + subclassTypeId + "\",\"x\":42}}",
                subclassTypeId);
    }

    @Test
    public void shouldRejectDisallowedCollectionElementWithoutInitializingClassV1() {
        final String typeId = StaticInitCanaryElement.class.getName();
        final String json = "{\"@class\":\"java.util.HashMap\",\"v\":[\"java.util.ArrayList\",["
                + "{\"@class\":\"" + typeId + "\",\"x\":1}]]}";

        System.clearProperty(StaticInitCanaryElement.FIRED_PROPERTY);
        assertTypeIdDeniedByTypeValidator(v1Typed(), json, typeId);
        assertNull("type id resolution must not initialize the class named by a refused collection element",
                System.getProperty(StaticInitCanaryElement.FIRED_PROPERTY));
    }

    @Test
    public void shouldRejectEnumTypeParameterAndNotLoadItV1() {
        System.clearProperty(StaticInitCanaryEnum.FIRED_PROPERTY);
        assertDeniedByTypeValidator(v1Typed(),
                "{\"@class\":\"java.util.HashMap<com.example.gadget.GraphSONTestGadgets$StaticInitCanaryEnum,java.lang.String>\",\"A\":\"v\"}");
        assertNull("type id resolution must not load the enum named as a type argument",
                System.getProperty(StaticInitCanaryEnum.FIRED_PROPERTY));
    }

    @Test
    public void shouldRejectClassValueAndNotLoadItV1() {
        // java.lang.Class is held out of the derived names by graphSON1dDerivedTypeNames()
        System.clearProperty(StaticInitCanaryValue.FIRED_PROPERTY);
        assertDeniedByTypeValidator(v1Typed(),
                "{\"@class\":\"java.util.HashMap\",\"c\":[\"java.lang.Class\",\"com.example.gadget.GraphSONTestGadgets$StaticInitCanaryValue\"]}");
        assertNull("type id resolution must not load the class named by a java.lang.Class value",
                System.getProperty(StaticInitCanaryValue.FIRED_PROPERTY));
    }

    @Test
    public void shouldNotLoadDisallowedClassWhenRefusingV1() {
        // a @class outside the allowed names is decided from the name alone
        System.clearProperty(StaticInitCanary.FIRED_PROPERTY);
        assertDeniedByTypeValidator(v1Typed(),
                "{\"@class\":\"java.util.HashMap\",\"g\":{\"@class\":\"com.example.gadget.GraphSONTestGadgets$StaticInitCanary\",\"x\":1}}");
        assertNull("type id resolution must not load the class named by a refused @class",
                System.getProperty(StaticInitCanary.FIRED_PROPERTY));
    }

    @Test
    public void shouldRoundTripInetAddressWithEmbedTypeSettingV1() throws Exception {
        // java.net.InetAddress is a derived name, since GraphSON 2.0/3.0 register it
        final ObjectMapper mapper = v1Typed();
        final Map<String, Object> m = new HashMap<>();
        m.put("a", java.net.InetAddress.getByAddress(new byte[]{127, 0, 0, 1}));

        final Map read = mapper.readValue(mapper.writeValueAsString(m), HashMap.class);
        assertEquals(java.net.InetAddress.getByAddress(new byte[]{127, 0, 0, 1}), read.get("a"));
    }

    @Test
    public void shouldRoundTripUriWithEmbedTypeSettingV1() throws Exception {
        // java.net.URI is listed rather than derived, as GraphSON 2.0/3.0 do not register it
        final ObjectMapper mapper = v1Typed();
        final Map<String, Object> m = new HashMap<>();
        m.put("u", new java.net.URI("http://example.com/x"));

        final Map read = mapper.readValue(mapper.writeValueAsString(m), HashMap.class);
        assertEquals(new java.net.URI("http://example.com/x"), read.get("u"));
    }

    @Test
    public void shouldRoundTripBoxedPrimitivesWithEmbedTypeSettingV1() throws Exception {
        // String, Integer, Double and Boolean are written bare, so only the boxed types JSON cannot represent
        // natively carry a type id
        assertRoundTripsInMap(v1Typed(), Arrays.asList(
                Character.valueOf('c'),
                BigDecimal.ONE));
    }

    @Test
    public void shouldRoundTripCollectionTypesWithEmbedTypeSettingV1() throws Exception {
        // the concrete collection class names GraphSON 1.0 writes for a Map-nested value. Each case checks the type
        // id in the text written as well as the class read back, since AbstractMap.equals and AbstractList.equals
        // are structural and would keep passing with no type id written at all.
        final ObjectMapper mapper = v1Typed();
        final Map<String, Object> entry = Collections.singletonMap("a", "b");
        final List<String> one = Collections.singletonList("a");

        assertTypedRoundTripInMap(mapper, new LinkedHashMap<>(entry));
        assertTypedRoundTripInMap(mapper, new LinkedHashSet<>(one));

        // Jackson cannot rebuild either class named, so it reads the type id back through a stand-in it can
        // construct: Arrays.asList comes back as a plain ArrayList, and either unmodifiable list name comes back as a
        // wrapper around an ArrayList. unmodifiableList writes Collections$UnmodifiableList over a LinkedList but
        // Collections$UnmodifiableRandomAccessList over an ArrayList, so the allowed names carry both.
        assertTypedRoundTripInMap(mapper, Arrays.asList("a", "b"), ArrayList.class);
        assertTypedRoundTripInMap(mapper, Collections.unmodifiableList(new LinkedList<>(one)),
                Collections.unmodifiableList(new ArrayList<>(one)).getClass());

        // a Map in a List in a Map, so a type id is resolved at every depth
        final Map<String, Object> nested = new HashMap<>();
        nested.put("inner", new ArrayList<>(Collections.singletonList(new HashMap<String, Object>(entry))));
        final String nestedJson = writeInMap(mapper, nested);
        assertThat(nestedJson, containsString("\"inner\":[\"java.util.ArrayList\",["));

        final Object readNested = readMapValue(mapper, nestedJson);
        assertEquals(nested, readNested);
        assertEquals(HashMap.class, readNested.getClass());
        final Object readInner = ((Map<?, ?>) readNested).get("inner");
        assertEquals(ArrayList.class, readInner.getClass());
        assertEquals(HashMap.class, ((List<?>) readInner).get(0).getClass());
    }

    @Test
    public void shouldRoundTripEnumTypesWithEmbedTypeSettingV1() throws Exception {
        // T uses per-constant subclasses, whose type id is still the declaring enum. DayOfWeek is listed in
        // GRAPHSON_1_0_ALLOWED_EXTRA_TYPE_NAMES rather than derived from the GraphSON 2.0/3.0 registry.
        assertRoundTripsInMap(v1Typed(), Arrays.asList(
                Direction.OUT,
                T.id,
                DayOfWeek.MONDAY));
    }

    @Test
    public void shouldReadAllowedBaseTypeIdsWithEmbedTypeSettingV1() throws Exception {
        // allowed names GraphSON 1.0 does not write itself, since it writes the concrete runtime class instead. A
        // document may still name them, and what comes back is the concrete type Jackson picks for the base type.
        final ObjectMapper mapper = v1Typed();

        assertEquals(Collections.singletonMap("a", "b"),
                readMapValue(mapper, "{\"@class\":\"java.util.HashMap\",\"v\":{\"@class\":\"java.util.Map\",\"a\":\"b\"}}"));
        assertEquals(ByteBuffer.wrap(new byte[]{1, 2}),
                readMapValue(mapper, "{\"@class\":\"java.util.HashMap\",\"v\":[\"java.nio.ByteBuffer\",\"AQI=\"]}"));
    }

    @Test
    public void shouldRefuseStarGraphBecauseV1WritesItsStarVertexAsAMapV1() throws Exception {
        // GraphSON 1.0 writes a StarGraph as a bean whose "starVertex" property carries the type id
        // java.util.HashMap rather than a StarVertex, so Jackson cannot rebuild a StarGraph from what GraphSON 1.0
        // writes and the name is not among the allowed names.
        final ObjectMapper mapper = v1Typed();
        final String json;
        try (final StarGraph starGraph = StarGraph.open()) {
            starGraph.addVertex("label", "person");
            json = writeInMap(mapper, starGraph);
        }

        assertThat(json, containsString("\"" + GraphSONTokens.CLASS + "\":\"" + StarGraph.class.getName() + "\""));
        assertThat(json, containsString("\"starVertex\":{\"" + GraphSONTokens.CLASS + "\":\"java.util.HashMap\""));
        assertTypeIdDeniedByTypeValidator(mapper, json, StarGraph.class.getName());
    }

    @Test
    public void shouldRefuseByteBufferBecauseV1WritesItsConcreteHeapClassV1() throws Exception {
        // java.nio.ByteBuffer is a derived name and does resolve (see
        // shouldReadAllowedBaseTypeIdsWithEmbedTypeSettingV1), but what GraphSON 1.0 writes for wrap() or allocate()
        // is the concrete java.nio.HeapByteBuffer, which the allowed names do not carry, so V1 cannot read what it
        // writes.
        final ObjectMapper mapper = v1Typed();

        final String json = writeInMap(mapper, ByteBuffer.wrap(new byte[]{1, 2}));
        assertThat(json, containsString("[\"java.nio.HeapByteBuffer\","));
        assertTypeIdDeniedByTypeValidator(mapper, json, "java.nio.HeapByteBuffer");
    }

    @Test
    public void shouldRefuseEnumMapBecauseV1WritesAParameterizedTypeIdV1() throws Exception {
        // GraphSON 1.0 writes an EnumMap as the parameterized type id "java.util.EnumMap<...,...>", which
        // GraphSON1dScreeningIdResolver refuses, so an EnumMap no longer reads back. That is a round-trip regression
        // introduced by this change. An EnumSet is written as a parameterized type id too, but it did not read back
        // beforehand either, as jackson-databind#4849 leaves the type id EnumSet writes unresolvable.
        final ObjectMapper mapper = v1Typed();
        final String typeId = "java.util.EnumMap<" + Direction.class.getName() + ",java.lang.Object>";

        final EnumMap<Direction, String> value = new EnumMap<>(Direction.class);
        value.put(Direction.OUT, "x");

        final String json = writeInMap(mapper, value);
        assertThat(json, containsString("\"" + GraphSONTokens.CLASS + "\":\"" + typeId + "\""));
        assertParameterizedTypeIdRefused(mapper, json, typeId);
    }

    @Test
    public void shouldRejectUnresolvableHostnameWithoutLookupWithEmbedTypeSettingV1() {
        // java.net.InetAddress is among the allowed names, so a document may name it, but a value that is not an IP
        // address literal is refused on its syntax rather than looked up. The message is the evidence of that, as a
        // lookup would report an UnknownHostException instead. It is Jackson's own wording, so an upgrade can move it.
        final String json = "{\"@class\":\"java.util.HashMap\",\"v\":" +
                "[\"java.net.InetAddress\",\"this-name-should-not-resolve.invalid\"]}";
        try {
            v1Typed().readValue(json, HashMap.class);
            fail("an InetAddress value that is not an IP address literal must be refused");
        } catch (Exception e) {
            assertThat(e, instanceOf(InvalidFormatException.class));
            assertThat(e.getMessage(), containsString("Not a valid IP address string literal"));
        }
    }

    @Test
    public void shouldAllowConfiguredClassValueWithEmbedTypeSettingV1() throws Exception {
        // java.lang.Class is held out of the derived names, and no separate list holds it out permanently, so a
        // caller that wants GraphSON 1.0 to read one names it like any other name
        final String json = "{\"@class\":\"java.util.HashMap\",\"c\":[\"java.lang.Class\",\"java.lang.String\"]}";
        assertDeniedByTypeValidator(v1Typed(), json);

        final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES).addAllowedTypeIdName("java.lang.Class").create().createMapper();
        assertEquals(String.class, mapper.readValue(json, HashMap.class).get("c"));
    }

    @Test
    public void shouldAllowSeveralConfiguredTypeIdNamesWithEmbedTypeSettingV1() throws Exception {
        // addAllowedTypeIdName takes several names at once, and successive calls add to earlier ones rather than
        // replacing them
        final String json = "{\"@class\":\"java.util.HashMap\","
                + "\"p\":{\"@class\":\"com.example.gadget.GraphSONTestGadgets$SamplePojo\",\"x\":42},"
                + "\"u\":[\"java.net.URL\",\"http://example.com/x\"]}";

        assertDeniedByTypeValidator(v1Typed(), json);

        assertConfiguredNamesResolve(GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName(SamplePojo.class.getName(), "java.net.URL")
                .create().createMapper(), json);

        assertConfiguredNamesResolve(GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName(SamplePojo.class.getName())
                .addAllowedTypeIdName("java.net.URL")
                .create().createMapper(), json);
    }

    @Test
    public void shouldIgnoreDuplicateConfiguredTypeIdNamesWithEmbedTypeSettingV1() throws Exception {
        final String json = "{\"@class\":\"java.util.HashMap\","
                + "\"p\":{\"@class\":\"com.example.gadget.GraphSONTestGadgets$SamplePojo\",\"x\":42}}";

        final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES)
                .addAllowedTypeIdName(SamplePojo.class.getName())
                .addAllowedTypeIdName(SamplePojo.class.getName())
                .create().createMapper();

        assertEquals(new SamplePojo(42), mapper.readValue(json, HashMap.class).get("p"));
    }

    @Test
    public void shouldDeriveTypeIdNamesFromTheRegisteredGraphSON2And3TypesV1() {
        // a new put(...) in GraphSONModuleV2, GraphSONModuleV3, GraphSONXModuleV2 or GraphSONXModuleV3 widens what
        // GraphSON 1.0 reads as a side effect, which pinning the derived names makes visible.
        //
        // The set is classpath dependent: GraphSONModule.tryLoadSparqlStrategy() contributes SparqlStrategy when
        // sparql-gremlin is present, which it is not on the gremlin-core test classpath. A new name that is genuinely
        // wanted belongs in the expected set below.
        final Set<String> expected = new TreeSet<>(Arrays.asList(
                "java.lang.Byte",
                "java.lang.Character",
                "java.lang.Double",
                "java.lang.Float",
                "java.lang.Integer",
                "java.lang.Long",
                "java.lang.Short",
                "java.math.BigDecimal",
                "java.math.BigInteger",
                "java.net.InetAddress",
                "java.nio.ByteBuffer",
                "java.sql.Timestamp",
                "java.time.Duration",
                "java.time.Instant",
                "java.time.LocalDate",
                "java.time.LocalDateTime",
                "java.time.LocalTime",
                "java.time.MonthDay",
                "java.time.OffsetDateTime",
                "java.time.OffsetTime",
                "java.time.Period",
                "java.time.Year",
                "java.time.YearMonth",
                "java.time.ZoneOffset",
                "java.time.ZonedDateTime",
                "java.util.Calendar",
                "java.util.Date",
                "java.util.List",
                "java.util.Map",
                "java.util.Set",
                "java.util.TimeZone",
                "java.util.UUID",
                "org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy",
                "org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.Bytecode",
                "org.apache.tinkerpop.gremlin.process.traversal.Bytecode$Binding",
                "org.apache.tinkerpop.gremlin.process.traversal.DT",
                "org.apache.tinkerpop.gremlin.process.traversal.Merge",
                "org.apache.tinkerpop.gremlin.process.traversal.Operator",
                "org.apache.tinkerpop.gremlin.process.traversal.Order",
                "org.apache.tinkerpop.gremlin.process.traversal.P",
                "org.apache.tinkerpop.gremlin.process.traversal.Path",
                "org.apache.tinkerpop.gremlin.process.traversal.Pick",
                "org.apache.tinkerpop.gremlin.process.traversal.Pop",
                "org.apache.tinkerpop.gremlin.process.traversal.SackFunctions$Barrier",
                "org.apache.tinkerpop.gremlin.process.traversal.Scope",
                "org.apache.tinkerpop.gremlin.process.traversal.TextP",
                "org.apache.tinkerpop.gremlin.process.traversal.Traverser",
                "org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet",
                "org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ByModulatorOptimizationStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy",
                "org.apache.tinkerpop.gremlin.process.traversal.util.AndP",
                "org.apache.tinkerpop.gremlin.process.traversal.util.Metrics",
                "org.apache.tinkerpop.gremlin.process.traversal.util.OrP",
                "org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation",
                "org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics",
                "org.apache.tinkerpop.gremlin.structure.Column",
                "org.apache.tinkerpop.gremlin.structure.Direction",
                "org.apache.tinkerpop.gremlin.structure.Edge",
                "org.apache.tinkerpop.gremlin.structure.Property",
                "org.apache.tinkerpop.gremlin.structure.T",
                "org.apache.tinkerpop.gremlin.structure.Vertex",
                "org.apache.tinkerpop.gremlin.structure.VertexProperty",
                "org.apache.tinkerpop.gremlin.structure.VertexProperty$Cardinality",
                "org.apache.tinkerpop.gremlin.util.function.Lambda"));

        final Set<String> derived = new TreeSet<>(GraphSONMapper.graphSON1dDerivedTypeNames());
        final Set<String> added = new TreeSet<>(derived);
        added.removeAll(expected);
        final Set<String> dropped = new TreeSet<>(expected);
        dropped.removeAll(derived);
        assertEquals("derived but not expected: " + added + "; expected but not derived: " + dropped,
                expected, derived);
    }

    private static ObjectMapper v1Typed() {
        return GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper();
    }

    /**
     * Writes a value as a {@code Map} value and reads it back. That is the shape real payloads use, and it exercises
     * the untyped {@code Object} value path rather than a declared concrete class.
     */
    private static Object roundTripInMap(final ObjectMapper mapper, final Object value) throws Exception {
        return readMapValue(mapper, writeInMap(mapper, value));
    }

    /**
     * Writes a value as a {@code Map} value and returns the text, so the type id GraphSON 1.0 emitted for it can be
     * asserted on directly.
     */
    private static String writeInMap(final ObjectMapper mapper, final Object value) throws Exception {
        final Map<String, Object> m = new HashMap<>();
        m.put(MAP_VALUE_KEY, value);
        return mapper.writeValueAsString(m);
    }

    private static void assertRoundTripsInMap(final ObjectMapper mapper, final List<Object> values) throws Exception {
        for (final Object value : values) {
            assertEquals(value.getClass().getName(), value, roundTripInMap(mapper, value));
        }
    }

    private static void assertTypedRoundTripInMap(final ObjectMapper mapper, final Object value) throws Exception {
        assertTypedRoundTripInMap(mapper, value, value.getClass());
    }

    /**
     * Asserts that a {@code Map} or {@code Collection} value reads back equal and as {@code expectedClass}, and that
     * the type id GraphSON 1.0 wrote for it is in the text written. Equality alone is no evidence of typing, as an
     * equal value of any other concrete {@code Map} or {@code List} class satisfies it. {@code expectedClass} is what
     * Jackson rebuilds for the type id written, which is not always the class that was written.
     */
    private static void assertTypedRoundTripInMap(final ObjectMapper mapper, final Object value,
                                                  final Class<?> expectedClass) throws Exception {
        final String name = value.getClass().getName();
        final String json = writeInMap(mapper, value);
        assertThat("no type id written for " + name + " in " + json, json, containsString(typeIdInMapOf(value)));

        final Object read = readMapValue(mapper, json);
        assertEquals(name, value, read);
        assertEquals("concrete class read back for " + name, expectedClass, read.getClass());
    }

    /**
     * The text GraphSON 1.0 writes for the type id of a {@code Map} or {@code Collection} held as a {@code Map}
     * value: a {@code Map} carries it as an {@code "@class"} field of its own object, a {@code Collection} as the
     * first element of a wrapper array. Both are anchored to the key the value sits under, so neither can be
     * satisfied by the type id of the enclosing {@code Map}.
     */
    private static String typeIdInMapOf(final Object value) {
        final String prefix = "\"" + MAP_VALUE_KEY + "\":";
        final String name = value.getClass().getName();
        return value instanceof Map
                ? prefix + "{\"" + GraphSONTokens.CLASS + "\":\"" + name + "\""
                : prefix + "[\"" + name + "\",";
    }

    private static Object readMapValue(final ObjectMapper mapper, final String json) throws Exception {
        return mapper.readValue(json, HashMap.class).get(MAP_VALUE_KEY);
    }

    /**
     * Asserts that both configured names resolved. A {@code java.net.URL} is compared as text, since
     * {@code URL.equals} can consult the network.
     */
    private static void assertConfiguredNamesResolve(final ObjectMapper mapper, final String json) throws Exception {
        final Map read = mapper.readValue(json, HashMap.class);
        assertEquals(new SamplePojo(42), read.get("p"));
        assertEquals("http://example.com/x", read.get("u").toString());
    }

    private static void assertDeniedByTypeValidator(final ObjectMapper mapper, final String json) {
        try {
            mapper.readValue(json, HashMap.class);
            fail("a @class outside the allowed names must not resolve");
        } catch (InvalidTypeIdException expected) {
        } catch (Exception other) {
            throw new AssertionError("expected InvalidTypeIdException, got " + other, other);
        }
    }

    /**
     * Asserts that {@code typeId} in particular is what the read was refused on, and that the allowed names are what
     * refused it. A listed type id that names no loadable class is also an {@code InvalidTypeIdException}, reported as
     * "no such class found" rather than as a denial. Both strings matched are Jackson's own wording, so a Jackson
     * upgrade can move them.
     */
    private static void assertTypeIdDeniedByTypeValidator(final ObjectMapper mapper, final String json,
                                                          final String typeId) {
        try {
            mapper.readValue(json, HashMap.class);
            fail("resolution of the type id " + typeId + " must be refused");
        } catch (InvalidTypeIdException expected) {
            assertThat(expected.getMessage(), containsString("Could not resolve type id '" + typeId + "'"));
            assertThat(expected.getMessage(), containsString("denied resolution"));
        } catch (Exception other) {
            throw new AssertionError("expected InvalidTypeIdException, got " + other, other);
        }
    }

    /**
     * Asserts that {@code typeId} was refused for being parameterized, which {@code GraphSON1dScreeningIdResolver}
     * does before the allowed names are consulted at all.
     */
    private static void assertParameterizedTypeIdRefused(final ObjectMapper mapper, final String json,
                                                         final String typeId) {
        try {
            mapper.readValue(json, HashMap.class);
            fail("a parameterized type id must be refused: " + typeId);
        } catch (InvalidTypeIdException expected) {
            assertThat(expected.getMessage(), containsString("Could not resolve type id '" + typeId + "'"));
            assertThat(expected.getMessage(), containsString("GraphSON 1.0 does not permit a parameterized type id"));
        } catch (Exception other) {
            throw new AssertionError("expected InvalidTypeIdException, got " + other, other);
        }
    }

    @Test
    public void shouldNotHandleMapWithTypesUsingEmbedTypeSettingV1() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V1_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100, read.get("test"));
    }

    @Test
    public void shouldLooseTypesInfoWithGraphSONNoType() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create()
                .createMapper();

        final UUID uuid = UUID.randomUUID();
        final List<Object> myList = new ArrayList<>();

        final List<Object> myList2 = new ArrayList<>();
        myList2.add(UUID.randomUUID());
        myList2.add(33L);
        myList2.add(84);
        final Map<String,Object> map2 = new HashMap<>();
        map2.put("eheh", UUID.randomUUID());
        map2.put("normal", "normal");
        myList2.add(map2);

        final Map<String, Object> map1 = new HashMap<>();
        map1.put("hello", "world");
        map1.put("test", uuid);
        map1.put("hehe", myList2);
        myList.add(map1);

        myList.add("kjkj");
        myList.add(UUID.randomUUID());

        final String json = mapper.writeValueAsString(myList);
        final Object read = mapper.readValue(json, Object.class);

        // Not equals because of type loss
        assertNotEquals(myList, read);
    }

    @Test
    public void shouldHandleDefaultRemoteTraverser() throws Exception {
        final DefaultRemoteTraverser<String> o = new DefaultRemoteTraverser<>("test", 100);
        assertEquals(o, serializeDeserialize(mapper, o, Traverser.class));
    }

    @Test
    public void shouldHandleVariantsOfP() throws Exception {
        final List<P> variantsOfP = Arrays.asList(
                P.between(1,2),
                P.eq(1),
                P.gt(1),
                P.gte(1),
                P.inside(1,2),
                P.lt(1),
                P.lte(1),
                P.neq(1),
                P.not(P.eq(1)),
                P.outside(1,2),
                P.within(1),
                P.within(1,2,3,4),
                P.within(Arrays.asList(1,2,3,4)),
                P.without(1),
                P.without(1,2,3,4),
                P.without(Arrays.asList(1,2,3,4)),
                P.eq(1).and(P.eq(2)),
                P.eq(1).or(P.eq(2)),
                TextP.containing("ark"),
                TextP.startingWith("mar"),
                TextP.endingWith("ko"),
                TextP.endingWith("ko").and(P.gte("mar")),
                P.gte("mar").and(TextP.endingWith("ko")));

        for (P p : variantsOfP) {
            if (p instanceof TextP) {
                assertEquals(p, serializeDeserialize(mapper, p, TextP.class));
            } else {
                assertEquals(p, serializeDeserialize(mapper, p, P.class));
            }
        }
    }

    // Class needs to be defined as statics as it's a nested class.
    public static class FunObject {
        private String val;

        public FunObject() {
        }

        public String getVal() {
            return this.val;
        }

        public void setVal(String s) {
            this.val = s;
        }

        public String toString() {
            return this.val;
        }
    }


}
