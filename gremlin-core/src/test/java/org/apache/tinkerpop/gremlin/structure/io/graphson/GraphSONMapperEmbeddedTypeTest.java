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

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsNot.not;
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
                {"v1", GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0)
                        .addCustomModule(GraphSONXModuleV2.build())
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v3", GraphSONMapper.build().version(GraphSONVersion.V3_0)
                        .addCustomModule(GraphSONXModuleV3.build())
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v4", GraphSONMapper.build().version(GraphSONVersion.V4_0)
                        .addCustomModule(GraphSONXModuleV4.build())
                        .typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()}
        });
    }

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;


    @Parameterized.Parameter(0)
    public String version;

    @Test
    public void shouldHandleBoolean() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        final boolean b = true;
        assertEquals(b, serializeDeserialize(mapper, b, Boolean.class));
    }

    @Test
    public void shouldHandleString() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        final String s = "simple";
        assertEquals(s, serializeDeserialize(mapper, s, String.class));
    }

    @Test
    public void shouldHandleTraversalExplanation() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final TraversalExplanation o = __().out().outV().outE().explain();
        final TraversalExplanation deser = serializeDeserialize(mapper, o, TraversalExplanation.class);
        assertEquals(o.prettyPrint(), deser.prettyPrint());
    }

    @Test
    public void shouldHandleBulkSet() throws Exception {
        // only supported on V4
        assumeThat(version, not(anyOf(startsWith("v1"), startsWith("v2"), startsWith("v3"))));

        final BulkSet<String> bs = new BulkSet<>();
        bs.add("test1", 1);
        bs.add("test2", 2);
        bs.add("test3", 3);

        final List<String> expandedBs = new ArrayList<>(bs);
        assertEquals(expandedBs, serializeDeserializeAuto(mapper, bs));
    }

    @Test
    public void shouldHandleNumberConstants() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        final List<Object> o = new ArrayList<>();
        o.add(123.321d);
        o.add(Double.NaN);
        o.add(Double.NEGATIVE_INFINITY);
        o.add(Double.POSITIVE_INFINITY);

        assertEquals(o, serializeDeserialize(mapper, o, List.class));
    }

    @Test
    public void shouldHandleMap() throws Exception {
        assumeThat(version, either(startsWith("v3")).or(startsWith("v4")));

        final Map<Object,Object> o = new LinkedHashMap<>();
        o.put("string key", "string value");
        o.put(1, 1);
        o.put(1L, 1L);

        final List<Object> l = Arrays.asList("test", 1, 5L);
        o.put(l, "crazy");

        assertEquals(o, serializeDeserialize(mapper, o, Map.class));
    }

    @Test
    public void shouldHandleList() throws Exception {
        assumeThat(version, either(startsWith("v3")).or(startsWith("v4")));

        final List<Object> o = new ArrayList<>();
        o.add("test");
        o.add(1);
        o.add(1);
        o.add(1L);
        o.add(1L);

        final List<Object> l = Arrays.asList("test", 1, 5L);
        o.add(l);

        assertEquals(o, serializeDeserialize(mapper, o, List.class));
    }

    @Test
    public void shouldHandleSet() throws Exception {
        assumeThat(version, either(startsWith("v3")).or(startsWith("v4")));

        final Set<Object> o = new LinkedHashSet<>();
        o.add("test");
        o.add(1);
        o.add(1);
        o.add(1L);
        o.add(1L);

        final List<Object> l = Arrays.asList("test", 1, 5L);
        o.add(l);

        assertEquals(o, serializeDeserialize(mapper, o, Set.class));

    }

    @Test
    public void shouldHandleBiFunctionLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.biFunction("x,y -> 'test'");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleComparatorLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.comparator("x,y -> x <=> y");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleConsumerLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.consumer("x -> x");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleFunctionLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.function("x -> x");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandlePredicateLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.predicate("x -> true");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleSupplierLambda() throws Exception {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final Lambda o = (Lambda) Lambda.supplier("'test'");
        assertEquals(o, serializeDeserialize(mapper, o, Lambda.class));
    }

    @Test
    public void shouldHandleDuration() throws Exception  {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserialize(mapper, o, Duration.class));
    }

    @Test
    public void shouldHandleInstant() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserialize(mapper, o, Instant.class));
    }

    @Test
    public void shouldHandleLocalDate() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalDate.class));
    }

    @Test
    public void shouldHandleLocalDateTime() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalDateTime.class));
    }

    @Test
    public void shouldHandleLocalTime() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, LocalTime.class));
    }

    @Test
    public void shouldHandleMonthDay() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

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
        assumeThat(version, not(startsWith("v4")));

        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, OffsetTime.class));
    }

    @Test
    public void shouldHandlePeriod() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserialize(mapper, o, Period.class));
    }

    @Test
    public void shouldHandleYear() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Year o = Year.now();
        assertEquals(o, serializeDeserialize(mapper, o, Year.class));
    }

    @Test
    public void shouldHandleYearMonth() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserialize(mapper, o, YearMonth.class));
    }

    @Test
    public void shouldHandleZonedDateTime() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserialize(mapper, o, ZonedDateTime.class));
    }

    @Test
    public void shouldHandleZonedOffset() throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final ZoneOffset o  = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserialize(mapper, o, ZoneOffset.class));
    }

    @Test
    public void shouldHandleBigInteger() throws Exception  {
        assumeThat(version, not(startsWith("v1")));
        
        final BigInteger o = new BigInteger("123456789987654321123456789987654321");
        assertEquals(o, serializeDeserialize(mapper, o, BigInteger.class));
    }

    @Test
    public void shouldReadBigIntegerAsString() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        final BigInteger o = new BigInteger("123456789987654321123456789987654321");
        if (version.startsWith("v4")) {
            assertEquals(o, mapper.readValue("{\"@type\": \"g:BigInteger\", \"@value\": \"123456789987654321123456789987654321\"}", Object.class));
        } else {
            assertEquals(o, mapper.readValue("{\"@type\": \"gx:BigInteger\", \"@value\": \"123456789987654321123456789987654321\"}", Object.class));
        }
    }

    @Test
    public void shouldReadBigIntegerAsNumber() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        // this was the original GraphSON 2.0/3.0 format published for BigInteger but jackson is flexible enough to
        // do it as a string. the string approach is probably better for most language variants so while this tests
        // enforces this approach but leaves open the opportunity to accept either. at some point in the future
        // perhaps it can switch fully - TINKERPOP-2156
        final BigInteger o = new BigInteger("123456789987654321123456789987654321");
        if (version.startsWith("v4")) {
            assertEquals(o, mapper.readValue("{\"@type\": \"g:BigInteger\", \"@value\": 123456789987654321123456789987654321}", Object.class));
        } else {
            assertEquals(o, mapper.readValue("{\"@type\": \"gx:BigInteger\", \"@value\": 123456789987654321123456789987654321}", Object.class));
        }
    }

    @Test
    public void shouldHandleBigDecimal() throws Exception  {
        assumeThat(version, not(startsWith("v1")));

        final BigDecimal o = new BigDecimal("123456789987654321123456789987654321");
        assertEquals(o, serializeDeserialize(mapper, o, BigDecimal.class));
    }

    @Test
    public void shouldHandlePMultiValue() throws Exception  {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = P.within(1,2,3,null);
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    @Test
    public void shouldHandlePSingleValue() throws Exception  {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = P.within(1);
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    @Test
    public void shouldHandlePMultiValueAsList() throws Exception  {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = P.within(Arrays.asList(1,2,3,null));
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    @Test
    public void shouldHandlePMultiValueAsSet() throws Exception  {
        assumeThat(version, startsWith("v3"));

        final P o = P.within(new HashSet<>(Arrays.asList(1,2,3)));
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    @Test
    public void shouldHandlePBetween() throws Exception  {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = P.between(1, 100);
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    @Test
    public void shouldReadPWithJsonArray() throws Exception {
        // for some reason v3 is forgiving about the naked json array - leaving this here for backward compatibility,
        // but should be a g:List (i think)
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = P.within(Arrays.asList(1,2,3));
        assertEquals(o, mapper.readValue("{\"@type\": \"g:P\", \"@value\": {\"predicate\": \"within\", \"value\": [{\"@type\": \"g:Int32\", \"@value\": 1},{\"@type\": \"g:Int32\", \"@value\": 2},{\"@type\": \"g:Int32\", \"@value\": 3}]}}", Object.class));
    }

    @Test
    public void shouldReadPWithGraphSONList() throws Exception {
        assumeThat(version, startsWith("v3"));

        final P o = P.within(Arrays.asList(1,2,3));
        assertEquals(o, mapper.readValue("{\"@type\": \"g:P\", \"@value\": {\"predicate\": \"within\", \"value\": {\"@type\": \"g:List\", \"@value\": [{\"@type\": \"g:Int32\", \"@value\": 1},{\"@type\": \"g:Int32\", \"@value\": 2},{\"@type\": \"g:Int32\", \"@value\": 3}]}}}", Object.class));
    }

    @Test
    public void shouldReadBigDecimalAsString() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        final BigDecimal o = new BigDecimal("123456789987654321123456789987654321");

        if (version.startsWith("v4")) {
            assertEquals(o, mapper.readValue("{\"@type\": \"g:BigDecimal\", \"@value\": \"123456789987654321123456789987654321\"}", Object.class));
        } else {
            assertEquals(o, mapper.readValue("{\"@type\": \"gx:BigDecimal\", \"@value\": \"123456789987654321123456789987654321\"}", Object.class));
        }
    }

    @Test
    public void shouldReadBigDecimalAsNumber() throws Exception {
        assumeThat(version, not(startsWith("v1")));

        // this was the original GraphSON 2.0/3.0 format published for BigDecimal but jackson is flexible enough to
        // do it as a string. the string approach is probably better for most language variants so while this tests
        // enforces this approach but leaves open the opportunity to accept either. at some point in the future
        // perhaps it can switch fully - TINKERPOP-2156
        final BigDecimal o = new BigDecimal("123456789987654321123456789987654321");
        if (version.startsWith("v4")) {
            assertEquals(o, mapper.readValue("{\"@type\": \"g:BigDecimal\", \"@value\": 123456789987654321123456789987654321}", Object.class));
        } else {
            assertEquals(o, mapper.readValue("{\"@type\": \"gx:BigDecimal\", \"@value\": 123456789987654321123456789987654321}", Object.class));
        }
    }

    @Test
    public void shouldHandlePExt() throws Exception  {
        assumeThat(version, either(startsWith("v2")).or(startsWith("v3")));

        final P o = PExt.mix("bah");
        assertEquals(o, serializeDeserialize(mapper, o, P.class));
    }

    public static class PExt<V> extends P<V> {
        public PExt(final PBiPredicate<V, V> biPredicate, final V value) {
            super(biPredicate, value);
        }

        public static <V> P<V> mix(final V value) {
            return new P(Compare.eq, value);
        }
    }
}
