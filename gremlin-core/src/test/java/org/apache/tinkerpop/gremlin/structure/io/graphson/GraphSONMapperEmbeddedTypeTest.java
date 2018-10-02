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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.rules.TestName;

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
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONMapperEmbeddedTypeTest extends AbstractGraphSONTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v1", GraphSONMapper.build().version(GraphSONVersion.V1_0).embedTypes(true).create().createMapper()},
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0)
                        .addCustomModule(GraphSONXModuleV2d0.build().create(false))
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
        });
    }

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;


    @Parameterized.Parameter(0)
    public String version;

    @Test
    public void shouldHandleNumberConstants() throws Exception {
        assumeThat(version, startsWith("v2"));

        final List<Object> o = new ArrayList<>();
        o.add(123.321d);
        o.add(Double.NaN);
        o.add(Double.NEGATIVE_INFINITY);
        o.add(Double.POSITIVE_INFINITY);

        assertEquals(o, serializeDeserialize(mapper, o, List.class));
    }

    @Test
    public void shouldHandleBiFunctionLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.biFunction("x,y -> 'test'");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleComparatorLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.comparator("x,y -> x <=> y");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleConsumerLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.consumer("x -> x");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleFunctionLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.function("x -> x");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandlePredicateLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.predicate("x -> true");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleSupplierLambda() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Lambda o = (Lambda) Lambda.supplier("'test'");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleBytecodeBinding() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Bytecode.Binding<String> o = new Bytecode.Binding<>("test", "testing");
        assertEquals(o, serializeDeserialize(mapper, o, Bytecode.Binding.class));
    }

    @Test
    public void shouldHandleTraverser() throws Exception {
        assumeThat(version, startsWith("v2"));

        final Traverser<String> o = new DefaultRemoteTraverser<>("test", 100);
        assertEquals(o, serializeDeserialize(mapper, o, Traverser.class));
    }

    @Test
    public void shouldHandleDuration() throws Exception  {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserialize(mapper, o, Duration.class));
    }

    @Test
    public void shouldHandleInstant() throws Exception  {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserialize(mapper, o, Instant.class));
    }

    @Test
    public void shouldHandleLocalDate() throws Exception  {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalDate.class));
    }

    @Test
    public void shouldHandleLocalDateTime() throws Exception  {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalDateTime.class));
    }

    @Test
    public void shouldHandleLocalTime() throws Exception  {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalTime.class));
    }

    @Test
    public void shouldHandleMonthDay() throws Exception  {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserialize(mapper, o, MonthDay.class));
    }

    @Test
    public void shouldHandleOffsetDateTime() throws Exception  {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, OffsetDateTime.class));
    }

    @Test
    public void shouldHandleOffsetTime() throws Exception  {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, OffsetTime.class));
    }

    @Test
    public void shouldHandlePeriod() throws Exception  {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserialize(mapper, o, Period.class));
    }

    @Test
    public void shouldHandleYear() throws Exception  {
        final Year o = Year.now();
        assertEquals(o, serializeDeserialize(mapper, o, Year.class));
    }

    @Test
    public void shouldHandleYearMonth() throws Exception  {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserialize(mapper, o, YearMonth.class));
    }

    @Test
    public void shouldHandleZonedDateTime() throws Exception  {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, ZonedDateTime.class));
    }

    @Test
    public void shouldHandleZonedOffset() throws Exception  {
        final ZoneOffset o  = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserialize(mapper, o, ZoneOffset.class));
    }
}
