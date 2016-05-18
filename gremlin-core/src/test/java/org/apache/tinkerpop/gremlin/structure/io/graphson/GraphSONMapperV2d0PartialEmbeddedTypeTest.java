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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Tests automatic typed serialization/deserialization for GraphSON 2.0.
 */
public class GraphSONMapperV2d0PartialEmbeddedTypeTest {

    private final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .typeInfo(TypeInfo.PARTIAL_TYPES)
            .create()
            .createMapper();

    @Test
    public void shouldHandleDurationAuto() throws Exception {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleInstantAuto() throws Exception {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalDateAuto() throws Exception {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalDateTimeAuto() throws Exception {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalTimeAuto() throws Exception {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleMonthDayAuto() throws Exception {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleOffsetDateTimeAuto() throws Exception {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleOffsetTimeAuto() throws Exception {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandlePeriodAuto() throws Exception {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleYearAuto() throws Exception {
        final Year o = Year.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleYearMonthAuto() throws Exception {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleZonedDateTimeAuto() throws Exception {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleZonedOffsetAuto() throws Exception {
        final ZoneOffset o = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    // Trying to fail the TypeDeserializer type detection
    public void shouldSerializeDeserializeNestedCollectionsAndMapAndTypedValuesCorrectly() throws Exception {
        UUID uuid = UUID.randomUUID();
        List myList = new ArrayList<>();

        List myList2 = new ArrayList<>();
        myList2.add(UUID.randomUUID());
        myList2.add(33L);
        myList2.add(84);
        Map map2 = new HashMap<>();
        map2.put("eheh", UUID.randomUUID());
        map2.put("normal", "normal");
        myList2.add(map2);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("hello", "world");
        map1.put("test", uuid);
        map1.put("hehe", myList2);
        myList.add(map1);

        myList.add("kjkj");
        myList.add(UUID.randomUUID());
        assertEquals(myList, serializeDeserializeAuto(myList));

        // no "@value" property
        String s = "{\""+GraphSONTokens.VALUETYPE+"\":\""+GraphSONTokens.GREMLIN_TYPE_DOMAIN+":uuid\", \"test\":2}";
        Map map = new LinkedHashMap<>();
        map.put(GraphSONTokens.VALUETYPE, "gremlin:uuid");
        map.put("test", 2);
        Object res = mapper.readValue(s, Object.class);
        assertEquals(map, res);

        // "@value" and "@type" property reversed
        s = "{\""+GraphSONTokens.VALUEPROP+"\":2, \""+ GraphSONTokens.VALUETYPE+"\":\""+GraphSONTokens.GREMLIN_TYPE_DOMAIN+":int64\"}";
        res = mapper.readValue(s, Object.class);
        assertEquals(res, 2L);
        assertEquals(res.getClass(), Long.class);

        // no "@type" property.
        s = "{\""+GraphSONTokens.VALUEPROP+"\":2, \"id\":2}";
        map = new LinkedHashMap<>();
        map.put(GraphSONTokens.VALUEPROP, 2);
        map.put("id", 2);
        res = mapper.readValue(s, Object.class);
        assertEquals(res, map);
    }

    @Test
    public void shouldFailIfMoreThanTwoPropertiesInATypePattern() {
        String s = "{\"" + GraphSONTokens.VALUEPROP + "\":2, \"" + GraphSONTokens.VALUETYPE + "\":\""+GraphSONTokens.GREMLIN_TYPE_DOMAIN+":int64\", \"hello\": \"world\"}";
        try {
            mapper.readValue(s, Object.class);
            fail("Should have failed deserializing because there's more than properties in the type.");
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("Detected the type pattern in the JSON payload but the map containing the types and values contains other fields. This is not allowed by the deserializer."));
        }
        s = "{\"" + GraphSONTokens.VALUETYPE + "\":\""+GraphSONTokens.GREMLIN_TYPE_DOMAIN+":int64\",\"" + GraphSONTokens.VALUEPROP + "\":2, \"hello\": \"world\"}";
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
            assertThat(e.getMessage(), containsString("Could not deserialize the JSON value as required. Nested exception: java.lang.InstantiationException: Cannot deserialize the value with the detected type contained in the JSON ('"+GraphSONTokens.GREMLIN_TYPE_DOMAIN+":zoneoffset') to the type specified in parameter to the object mapper (class java.time.Instant). Those types are incompatible."));
        }
    }

    @Test
    public void shouldHandleRawPOJOs() throws Exception {
        final FunObject funObject = new FunObject();
        funObject.setVal("test");
        assertEquals(funObject.toString(), serializeDeserialize(funObject, FunObject.class).toString());
        assertEquals(funObject.getClass(), serializeDeserialize(funObject, FunObject.class).getClass());
    }

    @Test
    public void shouldHandleMapWithTypesUsingEmbedTypeSetting() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .embedTypes(true)
                .create()
                .createMapper();

        final Map<String,Object> m = new HashMap<>();
        m.put("test", 100L);

        final String json = mapper.writeValueAsString(m);
        final Map read = mapper.readValue(json, HashMap.class);

        assertEquals(100L, read.get("test"));
    }

    @Test
    public void shouldNotHandleMapWithTypesUsingEmbedTypeSetting() throws Exception {
        final ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .embedTypes(false)
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
        ObjectMapper mapper = GraphSONMapper.build()
                .version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.NO_TYPES)
                .create()
                .createMapper();

        UUID uuid = UUID.randomUUID();
        List myList = new ArrayList<>();

        List myList2 = new ArrayList<>();
        myList2.add(UUID.randomUUID());
        myList2.add(33L);
        myList2.add(84);
        Map map2 = new HashMap<>();
        map2.put("eheh", UUID.randomUUID());
        map2.put("normal", "normal");
        myList2.add(map2);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("hello", "world");
        map1.put("test", uuid);
        map1.put("hehe", myList2);
        myList.add(map1);

        myList.add("kjkj");
        myList.add(UUID.randomUUID());

        String json = mapper.writeValueAsString(myList);
        Object read = mapper.readValue(json, Object.class);

        // Not equals because of type loss
        assertNotEquals(myList, read);
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

    public <T> T serializeDeserialize(final Object o, final Class<T> clazz) throws Exception {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            mapper.writeValue(stream, o);

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                return mapper.readValue(inputStream, clazz);
            }
        }
    }

    public <T> T serializeDeserializeAuto(final Object o) throws Exception {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            mapper.writeValue(stream, o);

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                // Object.class is the wildcard that triggers the auto discovery.
                return (T)mapper.readValue(inputStream, Object.class);
            }
        }
    }

}
