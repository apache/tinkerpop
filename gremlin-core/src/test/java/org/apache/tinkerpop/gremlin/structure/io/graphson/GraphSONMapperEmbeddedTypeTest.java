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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONMapperEmbeddedTypeTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {GraphSONMapper.build().version(GraphSONVersion.V1_0).embedTypes(true).create().createMapper()},
                {GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
        });
    }

    @Parameterized.Parameter
    public ObjectMapper mapper;

    @Test
    public void shouldHandleDuration()throws Exception  {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserialize(o, Duration.class));
    }
    @Test
    public void shouldHandleInstant()throws Exception  {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserialize(o, Instant.class));
    }

    @Test
    public void shouldHandleLocalDate()throws Exception  {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserialize(o, LocalDate.class));
    }

    @Test
    public void shouldHandleLocalDateTime()throws Exception  {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserialize(o, LocalDateTime.class));
    }

    @Test
    public void shouldHandleLocalTime()throws Exception  {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserialize(o, LocalTime.class));
    }

    @Test
    public void shouldHandleMonthDay()throws Exception  {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserialize(o, MonthDay.class));
    }

    @Test
    public void shouldHandleOffsetDateTime()throws Exception  {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetDateTime.class));
    }

    @Test
    public void shouldHandleOffsetTime()throws Exception  {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetTime.class));
    }

    @Test
    public void shouldHandlePeriod()throws Exception  {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserialize(o, Period.class));
    }

    @Test
    public void shouldHandleYear()throws Exception  {
        final Year o = Year.now();
        assertEquals(o, serializeDeserialize(o, Year.class));
    }

    @Test
    public void shouldHandleYearMonth()throws Exception  {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserialize(o, YearMonth.class));
    }

    @Test
    public void shouldHandleZonedDateTime()throws Exception  {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserialize(o, ZonedDateTime.class));
    }

    @Test
    public void shouldHandleZonedOffset()throws Exception  {
        final ZoneOffset o  = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserialize(o, ZoneOffset.class));
    }

    public <T> T serializeDeserialize(final Object o, final Class<T> clazz) throws Exception {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            mapper.writeValue(stream, o);

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                return mapper.readValue(inputStream, clazz);
            }
        }
    }
}
