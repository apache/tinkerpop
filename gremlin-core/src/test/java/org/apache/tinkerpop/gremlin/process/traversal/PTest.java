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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class PTest {

    @RunWith(Parameterized.class)
    public static class ParameterizedTest {
        @Rule
        public ExpectedException exceptionRule = ExpectedException.none();

        @Parameterized.Parameters(name = "{0}.test({1}) = {2}")
        public static Iterable<Object[]> data() {
            return new ArrayList<>(Arrays.asList(new Object[][]{
                    {P.eq(0), 0, true},
                    {P.eq(0), -0, true},
                    {P.eq(0), +0, true},
                    {P.eq(-0), +0, true},
                    {P.eq(0), 1, false},
                    {P.eq(0), null, false},
                    {P.eq((Object) null), null, true},
                    {P.eq((Object) null), 0, false},
                    {P.eq(Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, false},
                    {P.eq(Float.POSITIVE_INFINITY), Float.NEGATIVE_INFINITY, false},
                    {P.eq(Float.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, false},
                    {P.eq(GValue.of("x",0)), 0, true},
                    {P.eq(GValue.of("x",-0)), +0, true},
                    {P.eq(GValue.of("x",0)), 1, false},
                    {P.eq(GValue.of("x",0)), null, false},
                    {P.eq(GValue.of("x",null)), null, true},
                    {P.eq(GValue.of("x",null)), 0, false},
                    {P.neq(0), 0, false},
                    {P.neq(0), -0, false},
                    {P.neq(0), +0, false},
                    {P.neq(-0), +0, false},
                    {P.neq(0), 1, true},
                    {P.neq(0), null, true},
                    {P.neq((Object) null), null, false},
                    {P.neq((Object) null), 0, true},
                    {P.neq(Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, true},
                    {P.neq(Float.POSITIVE_INFINITY), Float.NEGATIVE_INFINITY, true},
                    {P.neq(Float.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, true},
                    {P.neq(GValue.of("x",0)), 0, false},
                    {P.neq(GValue.of("x",-0)), +0, false},
                    {P.neq(GValue.of("x",0)), 1, true},
                    {P.neq(GValue.of("x",0)), null, true},
                    {P.neq(GValue.of("x",null)), null, false},
                    {P.neq(GValue.of("x",null)), 0, true},
                    {P.gt(0), -1, false},
                    {P.gt(0), 0, false},
                    {P.gt(0), 1, true},
                    {P.gt(GValue.of("x",0)), -1, false},
                    {P.gt(GValue.of("x",0)), 0, false},
                    {P.gt(GValue.of("x",0)), 1, true},
                    {P.lt(0), -1, true},
                    {P.lt(0), 0, false},
                    {P.lt(0), 1, false},
                    {P.lt(GValue.of("x",0)), -1, true},
                    {P.lt(GValue.of("x",0)), 0, false},
                    {P.lt(GValue.of("x",0)), 1, false},
                    {P.lt(new int[]{1,2,3}), new int[]{0,1,2}, true},
                    {P.lt(new int[]{1,2,3}), new int[]{5,6,7}, false},
                    {P.lt(GValue.of("x", new int[]{1,2,3})), new int[]{0,1,2}, true},
                    {P.lt(GValue.of("x", new int[]{1,2,3})), new int[]{5,6,7}, false},
                    {P.gte(0), -1, false},
                    {P.gte(0), 0, true},
                    {P.gte(0), 1, true},
                    {P.gte(GValue.of("x",0)), -1, false},
                    {P.gte(GValue.of("x",0)), 0, true},
                    {P.gte(GValue.of("x",0)), 1, true},
                    {P.gte(new String[]{"a","b","c"}), new String[]{"x","y","z"}, true},
                    {P.gte(new String[]{"a","b","c"}), new String[]{"a","b","c"}, true},
                    {P.gte(new String[]{"a","b","c"}), new String[]{"a","B","z"}, false},
                    {P.gte(GValue.of("x",new String[]{"a","b","c"})), new String[]{"x","y","z"}, true},
                    {P.gte(GValue.of("x",new String[]{"a","b","c"})), new String[]{"a","b","c"}, true},
                    {P.gte(GValue.of("x",new String[]{"a","b","c"})), new String[]{"a","B","z"}, false},
                    {P.lte(0), -1, true},
                    {P.lte(0), 0, true},
                    {P.lte(0), 1, false},
                    {P.lte(GValue.of("x",0)), -1, true},
                    {P.lte(GValue.of("x",0)), 0, true},
                    {P.lte(GValue.of("x",0)), 1, false},
                    {P.lte(new double[]{0.0, 1.1, 2.2}), Arrays.asList(0.0, 1.1, 2.1), true},
                    {P.lte(new double[]{0.0, 1.1, 2.2}), Arrays.asList(0.0, 1.1, 2.3), false},
                    {P.lte(GValue.of("x",new double[]{0.0, 1.1, 2.2})), Arrays.asList(0.0, 1.1, 2.1), true},
                    {P.lte(GValue.of("x",new double[]{0.0, 1.1, 2.2})), Arrays.asList(0.0, 1.1, 2.3), false},
                    {P.between(1, 10), 0, false},
                    {P.between(1, 10), 1, true},
                    {P.between(1, 10), 9, true},
                    {P.between(1, 10), 10, false},
                    {P.between(GValue.of("x",1), GValue.of("y",10)), 0, false},
                    {P.between(GValue.of("x",1), GValue.of("y",10)), 1, true},
                    {P.between(GValue.of("x",1), 10), 9, true},
                    {P.between(1, GValue.of("y",10)), 10, false},
                    {P.inside(1, 10), 0, false},
                    {P.inside(1, 10), 1, false},
                    {P.inside(1, 10), 9, true},
                    {P.inside(1, 10), 10, false},
                    {P.inside(GValue.of("x",1), GValue.of("y",10)), 0, false},
                    {P.inside(GValue.of("x",1), GValue.of("y",10)), 1, false},
                    {P.inside(1, GValue.of("y",10)), 9, true},
                    {P.inside(GValue.of("x",1), 10), 10, false},
                    {P.outside(1, 10), 0, true},
                    {P.outside(1, 10), 1, false},
                    {P.outside(1, 10), 9, false},
                    {P.outside(1, 10), 10, false},
                    {P.outside(1, Double.NaN), 0, true},
                    {P.outside(GValue.of("x",1), GValue.of("y",10)), 0, true},
                    {P.outside(GValue.of("x",1), GValue.of("y",10)), 1, false},
                    {P.outside(1, GValue.of("y",10)), 9, false},
                    {P.outside(GValue.of("x",1), 10), 10, false},
                    {P.outside(GValue.of("x",1), GValue.of("y",Double.NaN)), 0, true},

                    {P.within(), 0, false},
                    {P.within((Object) null), 0, false},
                    {P.within((Object) null), null, true},
                    {P.within((Collection) null), 0, false},
                    {P.within((Collection) null), null, true},
                    {P.within(null, 2, 3), 0, false},
                    {P.within(null, 2, 3), null, true},
                    {P.within(null, 2, 3), 2, true},
                    {P.within(1, 2, 3), 0, false},
                    {P.within(1, 2, 3), 1, true},
                    {P.within(1, 2, 3), 10, false},
                    {P.within(Arrays.asList(null, 2, 3)), null, true},
                    {P.within(Arrays.asList(null, 2, 3)), 1, false},
                    {P.within(Arrays.asList(null, 2, 3)), 2, true},
                    {P.within(Arrays.asList(1, 2, 3)), 0, false},
                    {P.within(Arrays.asList(1, 2, 3)), 1, true},
                    {P.within(Arrays.asList(1, 2, 3)), 10, false},
                    {P.within(GValue.of("x", null)), 0, false},
                    {P.within(GValue.of("x", null)), null, true},
                    {P.within(GValue.of("x", (Collection) null)), 0, false},
                    {P.within(GValue.of("x", (Collection) null)), null, true},
                    {P.within(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), 0, false},
                    {P.within(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), null, true},
                    {P.within(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), 2, true},
                    {P.within(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 0, false},
                    {P.within(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 1, true},
                    {P.within(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 10, false},
                    {P.without(), 0, true},
                    {P.without((Object) null), 0, true},
                    {P.without((Object) null), null, false},
                    {P.without((Collection) null), 0, true},
                    {P.without((Collection) null), null, false},
                    {P.without(null, 2, 3), 0, true},
                    {P.without(null, 2, 3), null, false},
                    {P.without(null, 2, 3), 2, false},
                    {P.without(1, 2, 3), 0, true},
                    {P.without(1, 2, 3), 1, false},
                    {P.without(1, 2, 3), 10, true},
                    {P.without(Arrays.asList(null, 2, 3)), null, false},
                    {P.without(Arrays.asList(null, 2, 3)), 1, true},
                    {P.without(Arrays.asList(null, 2, 3)), 2, false},
                    {P.without(Arrays.asList(1, 2, 3)), 0, true},
                    {P.without(Arrays.asList(1, 2, 3)), 1, false},
                    {P.without(Arrays.asList(1, 2, 3)), 10, true},
                    {P.without(GValue.of("x", null)), 0, true},
                    {P.without(GValue.of("x", null)), null, false},
                    {P.without(GValue.of("x", (Collection) null)), 0, true},
                    {P.without(GValue.of("x", (Collection) null)), null, false},
                    {P.without(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), 0, true},
                    {P.without(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), null, false},
                    {P.without(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3)), 2, false},
                    {P.without(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 0, true},
                    {P.without(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 1, false},
                    {P.without(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3)), 10, true},
                    {P.without(Arrays.asList(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3))), null, false},
                    {P.without(Arrays.asList(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3))), 1, true},
                    {P.without(Arrays.asList(GValue.of("x", null), GValue.of("y", 2), GValue.of("z", 3))), 2, false},
                    {P.without(Arrays.asList(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3))), 0, true},
                    {P.without(Arrays.asList(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3))), 1, false},
                    {P.without(Arrays.asList(GValue.of("x", 1), GValue.of("y", 2), GValue.of("z", 3))), 10, true},
                    {P.between("m", "n").and(P.neq("marko")), "marko", false},
                    {P.between("m", "n").and(P.neq("marko")), "matthias", true},
                    {P.between("m", "n").or(P.eq("daniel")), "marko", true},
                    {P.between("m", "n").or(P.eq("daniel")), "daniel", true},
                    {P.between("m", "n").or(P.eq("daniel")), "stephen", false},
                    {P.between(GValue.of("x", "m"), GValue.of("y", "n")).and(P.neq(GValue.of("z", "marko"))), "marko", false},
                    {P.between(GValue.of("x", "m"), GValue.of("y", "n")).and(P.neq(GValue.of("z", "marko"))), "matthias", true},
                    {P.between(GValue.of("x", "m"), GValue.of("y", "n")).or(P.eq(GValue.of("z", "daniel"))), "marko", true},
                    {P.between(GValue.of("x", "m"), GValue.of("y", "n")).or(P.eq(GValue.of("z", "daniel"))), "daniel", true},
                    {P.between(GValue.of("x", "m"), GValue.of("y", "n")).or(P.eq(GValue.of("z", "daniel"))), "stephen", false},
                    {P.within().and(P.within()), 0, false},
                    {P.within().and(P.without()), 0, false},
                    {P.without().and(P.without()), 0, true},
                    {P.within().and(P.without()), 0, false},
                    {P.within().or(P.within()), 0, false},
                    {P.within().or(P.without()), 0, true},
                    {P.without().or(P.without()), 0, true},

                    {P.typeOf(GType.NUMBER), 1, true},
                    {P.typeOf(GType.STRING), "hello", true},
                    {P.typeOf(Boolean.class), false, true},
                    {P.typeOf(GType.NULL), null, true},
                    {P.typeOf(GType.NUMBER), "1", false},
                    {P.typeOf(GType.LIST), "list", false},
                    {P.typeOf("Number"), 1, true},
                    {P.typeOf("String"), 1, false},
                    {P.typeOf(GType.DURATION), Duration.ofSeconds(30), true},
                    {P.typeOf(GType.DURATION), "duration", false},
                    {P.typeOf(GType.CHAR), 'c', true},
                    {P.typeOf(GType.CHAR), "c", false},
                    {P.typeOf(GType.BINARY), ByteBuffer.wrap("some bytes for you".getBytes()), true},
                    {P.typeOf(GType.BINARY), "some non-bytes for you", false},

                    // text predicates
                    {TextP.containing("ark"), "marko", true},
                    {TextP.containing("ark"), "josh", false},
                    {TextP.containing(GValue.ofString("x", "ark")), "marko", true},
                    {TextP.containing(GValue.ofString("x", "ark")), "josh", false},
                    {TextP.startingWith("jo"), "marko", false},
                    {TextP.startingWith("jo"), "josh", true},
                    {TextP.startingWith(GValue.ofString("x", "jo")), "marko", false},
                    {TextP.startingWith(GValue.ofString("x", "jo")), "josh", true},
                    {TextP.endingWith("ter"), "marko", false},
                    {TextP.endingWith("ter"), "peter", true},
                    {TextP.endingWith(GValue.ofString("x", "ter")), "marko", false},
                    {TextP.endingWith(GValue.ofString("x", "ter")), "peter", true},
                    {TextP.containing("o"), "marko", true},
                    {TextP.containing("o"), "josh", true},
                    {TextP.containing("o").and(P.gte("j")), "marko", true},
                    {TextP.containing("o").and(P.gte("j")), "josh", true},
                    {TextP.containing("o").and(P.gte("j")).and(TextP.endingWith("ko")), "marko", true},
                    {TextP.containing("o").and(P.gte("j")).and(TextP.endingWith("ko")), "josh", false},
                    {TextP.containing("o").and(P.gte("j").and(TextP.endingWith("ko"))), "marko", true},
                    {TextP.containing("o").and(P.gte("j").and(TextP.endingWith("ko"))), "josh", false},
                    {TextP.containing(GValue.ofString("x", "o")), "marko", true},
                    {TextP.containing(GValue.ofString("x", "o")), "josh", true},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))), "marko", true},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))), "josh", true},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))).and(TextP.endingWith("ko")), "marko", true},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))).and(TextP.endingWith("ko")), "josh", false},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))).and(TextP.endingWith("ko")), "marko", true},
                    {TextP.containing(GValue.ofString("x", "o")).and(P.gte(GValue.ofString("y", "j"))).and(TextP.endingWith("ko")), "josh", false},

                    // non-comparable cases
                    {P.outside(Double.NaN, Double.NaN), 0, false},
                    {P.inside(-1, Double.NaN), 0, false},
                    {P.inside(Double.NaN, 1), 0, false},
                    {P.lt(Double.NaN), 0, false},
                    {TextP.containing((String) null), "abc", false},
                    {TextP.containing("abc"), null, false},
                    {TextP.containing((String) null), null, false},
                    {TextP.startingWith((String) null), "abc", false},
                    {TextP.startingWith("abc"), null, false},
                    {TextP.startingWith((String) null), null, false},
                    {TextP.endingWith((String) null), "abc", false},
                    {TextP.endingWith("abc"), null, false},
                    {TextP.endingWith((String) null), null, false},

                    // regex
                    {TextP.regex("D"), "Dallas Fort Worth", true},
                    {TextP.regex("d"), "Dallas Fort Worth", false},
                    {TextP.regex("^D"), "Dallas Fort Worth", true},
                    {TextP.regex("^d"), "Dallas Fort Worth", false},
                    {TextP.regex("^Da"), "Dallas Forth Worth", true},
                    {TextP.regex("^da"), "Dallas Forth Worth", false},
                    {TextP.regex("^x"), "Dallas Fort Worth", false},
                    {TextP.regex("s"), "Dallas Fort Worth", true},
                    {TextP.regex("x"), "Dallas Fort Worth", false},
                    {TextP.regex("Dal[l|x]as"), "Dallas Fort Worth", true},
                    {TextP.regex("Dal[f|x]as"), "Dallas Fort Worth", false},
                    {TextP.regex("[a-zA-Z]+ Fort"), "Dallas Fort Worth", true},
                    {TextP.regex("[1-9]{3}"), "123-ABC-456", true},
                    {TextP.regex("[1-9]{3}-[A-Z]{3}-[1-9]{3}"), "123-ABC-456", true},
                    {TextP.regex("[1-9]{3}-[a-z]{3}-[1-9]{3}"), "123-ABC-456", false},
                    {TextP.regex("(?i)[1-9]{3}-[a-z]{3}-[1-9]{3}"), "123-ABC-456", true},
                    {TextP.regex("(?i)abc"), "123-ABC-456", true},
                    {TextP.regex("(?i)[a-b]{3}-[1-9]{3}-[a-z]{3}"), "123-ABC-456", false},
                    {TextP.regex("Tinker.*\\u00A9"), "Apache TinkerPop©", true},
                    {TextP.regex(GValue.ofString("x", "(?i)[a-b]{3}-[1-9]{3}-[a-z]{3}")), "123-ABC-456", false},
                    {TextP.regex(GValue.ofString("x", "Tinker.*\\u00A9")), "Apache TinkerPop©", true},
                    {TextP.notRegex(GValue.ofString("x", "(?i)[a-b]{3}-[1-9]{3}-[a-z]{3}")), "123-ABC-456", true},
                    {TextP.notRegex(GValue.ofString("x", "Tinker.*\\u00A9")), "Apache TinkerPop©", false},
                    // Traversal-bearing predicates (resolved per-traverser before testing)
                    {P.eq(__.constant(0).asAdmin()), 0, true},
                    {P.eq(__.constant(0).asAdmin()), 1, false},
                    {P.neq(__.constant(0).asAdmin()), 1, true},
                    {P.neq(__.constant(0).asAdmin()), 0, false},
                    {P.gt(__.constant(0).asAdmin()), 1, true},
                    {P.gt(__.constant(0).asAdmin()), 0, false},
                    {P.gt(__.constant(0).asAdmin()), -1, false},
                    {P.lt(__.constant(0).asAdmin()), -1, true},
                    {P.lt(__.constant(0).asAdmin()), 0, false},
                    {P.lt(__.constant(0).asAdmin()), 1, false},
                    {P.gte(__.constant(0).asAdmin()), 0, true},
                    {P.gte(__.constant(0).asAdmin()), 1, true},
                    {P.gte(__.constant(0).asAdmin()), -1, false},
                    {P.lte(__.constant(0).asAdmin()), 0, true},
                    {P.lte(__.constant(0).asAdmin()), -1, true},
                    {P.lte(__.constant(0).asAdmin()), 1, false},
                    {P.within(__.inject(1, 2, 3).fold().asAdmin()), 2, true},
                    {P.within(__.inject(1, 2, 3).fold().asAdmin()), 5, false},
                    {P.without(__.inject(1, 2, 3).fold().asAdmin()), 5, true},
                    {P.without(__.inject(1, 2, 3).fold().asAdmin()), 2, false},
            }));
        }

        @Parameterized.Parameter(value = 0)
        public P predicate;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Parameterized.Parameter(value = 2)
        public Object expected;

        @Test
        public void shouldTest() {
            if (expected instanceof Class)
                exceptionRule.expect((Class) expected);

            if (predicate.hasTraversal()) predicate.resolve(new B_O_Traverser<>("test", 1L));

            assertEquals(expected, predicate.test(value));
            assertNotEquals(expected, predicate.clone().negate().test(value));
            assertNotEquals(expected, P.not(predicate.clone()).test(value));
            if (value instanceof Number) {
                assertEquals(expected, predicate.test(((Number) value).longValue()));
                assertNotEquals(expected, predicate.clone().negate().test(((Number) value).longValue()));
                assertNotEquals(expected, P.not(predicate).test(((Number) value).longValue()));
            }
        }

        @Before
        public void init() {
            // Skip value manipulation tests for traversal-bearing predicates since their
            // value is resolved at runtime, not set statically.
            if (predicate.hasTraversal()) {
                assertNotNull(predicate.hashCode());
                assertEquals(predicate, predicate.clone());
                return;
            }

            final Object pv = predicate.getValue();
            final Random r = new Random();
            assertNotNull(predicate.getBiPredicate());
            predicate.setValue(r.nextDouble());
            assertNotNull(predicate.getValue());
            predicate.setValue(pv);
            assertEquals(pv, predicate.getValue());
            assertNotNull(predicate.hashCode());
            assertEquals(predicate, predicate.clone());
            assertNotEquals(__.identity(), predicate);

            boolean thrown = true;
            try {
                predicate.and(new CustomPredicate());
                thrown = false;
            } catch (IllegalArgumentException ex) {
                assertEquals("Only P predicates can be and'd together", ex.getMessage());
            } finally {
                assertTrue(thrown);
            }

            thrown = true;
            try {
                predicate.or(new CustomPredicate());
                thrown = false;
            } catch (IllegalArgumentException ex) {
                assertEquals("Only P predicates can be or'd together", ex.getMessage());
            } finally {
                assertTrue(thrown);
            }
        }

        private class CustomPredicate implements Predicate {

            @Override
            public boolean test(Object o) {
                return false;
            }

            @Override
            public Predicate and(Predicate other) {
                return null;
            }

            @Override
            public Predicate negate() {
                return null;
            }

            @Override
            public Predicate or(Predicate other) {
                return null;
            }
        }
    }

    public static class ConnectiveTest {

        @Test
        public void shouldComposeCorrectly() {
            assertEquals(P.eq(1), P.eq(1));
            assertEquals(P.eq(1).and(P.eq(2)), new AndP<>(Arrays.asList(P.eq(1), P.eq(2))));
            assertEquals(P.eq(1).and(P.eq(2).and(P.eq(3))), new AndP<>(Arrays.asList(P.eq(1), P.eq(2), P.eq(3))));
            assertEquals(P.eq(1).and(P.eq(2).and(P.eq(3).and(P.eq(4)))), new AndP<>(Arrays.asList(P.eq(1), P.eq(2), P.eq(3), P.eq(4))));
            assertEquals(P.eq(1).or(P.eq(2).or(P.eq(3).or(P.eq(4)))), new OrP<>(Arrays.asList(P.eq(1), P.eq(2), P.eq(3), P.eq(4))));
            assertEquals(P.eq(1).or(P.eq(2).and(P.eq(3).or(P.eq(4)))), new OrP<>(Arrays.asList(P.eq(1), new AndP<>(Arrays.asList(P.eq(2), new OrP<>(Arrays.asList(P.eq(3), P.eq(4))))))));
            assertEquals(P.eq(1).and(P.eq(2).or(P.eq(3).and(P.eq(4)))), new AndP<>(Arrays.asList(P.eq(1), new OrP<>(Arrays.asList(P.eq(2), new AndP<>(Arrays.asList(P.eq(3), P.eq(4))))))));
            assertEquals(P.eq(1).and(P.eq(2).and(P.eq(3).or(P.eq(4)))), new AndP<>(Arrays.asList(P.eq(1), P.eq(2), new OrP<>(Arrays.asList(P.eq(3), P.eq(4))))));
        }
    }
    
    public static class SetValueTest {

        private static final int INITIAL_VALUE = 5;
        private static final int UPDATED_VALUE = 10;
        private static final String EQ_FORMAT = "eq(%d)";
        public static final String GT_FORMAT = "gt(%d)";
        public static final String LT_FORMAT = "lt(%d)";
        public static final String NOT_FORMAT = "not(%s)";

        @Test
        public void shouldUseUpdatedValueAfterSetValue() {
            P<Integer> predicate = P.eq(INITIAL_VALUE);
            assertTrue(predicate.test(INITIAL_VALUE));
            assertEquals(String.format(EQ_FORMAT, INITIAL_VALUE), predicate.toString());
            assertEquals(predicate, P.eq(INITIAL_VALUE));
            assertEquals(Integer.valueOf(INITIAL_VALUE), predicate.getValue());
            
            predicate.setValue(UPDATED_VALUE);
            assertTrue(predicate.test(UPDATED_VALUE));
            assertFalse(predicate.test(INITIAL_VALUE));
            assertEquals(String.format(EQ_FORMAT, UPDATED_VALUE), predicate.toString());
            assertEquals(predicate, P.eq(UPDATED_VALUE));
            assertNotEquals(predicate, P.eq(INITIAL_VALUE));
            assertEquals(Integer.valueOf(UPDATED_VALUE), predicate.getValue());
        }

        @Test
        public void shouldUseUpdatedValueAfterSetValueForNegation() {
            P<Integer> predicate = P.eq(INITIAL_VALUE);
            P<Integer> negated = predicate.negate();
            assertFalse(negated.test(INITIAL_VALUE));
            assertEquals(String.format(NOT_FORMAT, String.format(EQ_FORMAT, INITIAL_VALUE)), negated.toString());

            predicate.setValue(UPDATED_VALUE);
            P<Integer> updatedNegated = predicate.negate();
            assertTrue(updatedNegated.test(INITIAL_VALUE));
            assertFalse(updatedNegated.test(UPDATED_VALUE));
            assertEquals(String.format(NOT_FORMAT, String.format(EQ_FORMAT, UPDATED_VALUE)), updatedNegated.toString());
        }

        @Test
        public void shouldHandleNullValuesForSetValue() {
            P<Integer> predicate = P.eq(INITIAL_VALUE);
            predicate.setValue(null);
            
            assertTrue(predicate.test(null));
            assertFalse(predicate.test(INITIAL_VALUE));
            assertEquals("eq", predicate.toString());
            assertEquals(predicate, P.eq((Object) null));
            assertNotEquals(predicate, P.eq(INITIAL_VALUE));
        }

        @Test
        public void shouldUseUpdatedValueAfterSetValueForGreaterThan() {
            P<Integer> gtPredicate = P.gt(INITIAL_VALUE);
            gtPredicate.setValue(UPDATED_VALUE);
            assertTrue(gtPredicate.test(UPDATED_VALUE + 1));
            assertFalse(gtPredicate.test(UPDATED_VALUE));
            assertEquals(String.format(GT_FORMAT, UPDATED_VALUE), gtPredicate.toString());
        }

        @Test
        public void shouldUseUpdatedValueAfterSetValueForLessThan() {
            P<Integer> ltPredicate = P.lt(INITIAL_VALUE);
            ltPredicate.setValue(UPDATED_VALUE);
            assertTrue(ltPredicate.test(UPDATED_VALUE - 1));
            assertFalse(ltPredicate.test(UPDATED_VALUE));
            assertEquals(String.format(LT_FORMAT, UPDATED_VALUE), ltPredicate.toString());
        }

        @Test
        public void shouldUseUpdatedValueAfterSetValueContaining() {
            String initial = "old";
            String updated = "new";
            TextP textPredicate = TextP.containing(initial);
            textPredicate.setValue(updated);
            
            assertTrue(textPredicate.test(updated + "text"));
            assertFalse(textPredicate.test(initial + "text"));
            assertEquals(String.format("containing(%s)", updated), textPredicate.toString());
            assertEquals(textPredicate, TextP.containing(updated));
            assertNotEquals(textPredicate, TextP.containing(initial));
        }
    }

    /**
     * Tests that traversal detection in predicates is accurate: {@code P.hasTraversal()} returns
     * true for traversal-bearing predicates and false for literal/GValue predicates.
     */
    public static class TraversalDetectionTest {

        @Test
        public void shouldDetectTraversalInComparisonPredicate() {
            final P p = P.eq(__.constant(1).asAdmin());
            assertTrue(p.hasTraversal());
        }

        @Test
        public void shouldDetectTraversalInCollectionPredicate() {
            final P p = P.within(__.constant(1).asAdmin());
            assertTrue(p.hasTraversal());
        }

        @Test
        public void shouldNotDetectTraversalInLiteralPredicate() {
            final P p = P.eq(1);
            assertFalse(p.hasTraversal());
        }

        @Test
        public void shouldNotDetectTraversalInGValuePredicate() {
            final P p = P.eq(GValue.of("x", 1));
            assertFalse(p.hasTraversal());
        }

        @Test
        public void shouldReturnNullTraversalValueForLiteral() {
            final P p = P.eq(42);
            assertFalse(p.hasTraversal());
        }
    }

    /**
     * Tests that scalar predicates (eq, neq, gt, lt, gte, lte) take the first result from a
     * multi-result child traversal, and that collection predicates (within, without) accept
     * multiple results.
     */
    public static class TraversalResolutionTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        @Test
        public void shouldTakeFirstResultForEq() {
            final P p = P.eq(__.union(__.constant(1), __.constant(2)).asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(1));
            assertFalse(p.test(2));
        }

        @Test
        public void shouldTakeFirstResultForNeq() {
            final P p = P.neq(__.union(__.constant(1), __.constant(2)).asAdmin());
            p.resolve(createTraverser("start"));
            assertFalse(p.test(1));
            assertTrue(p.test(2));
        }

        @Test
        public void shouldTakeFirstResultForGt() {
            final P p = P.gt(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(11));
            assertFalse(p.test(10));
        }

        @Test
        public void shouldTakeFirstResultForLt() {
            final P p = P.lt(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(9));
            assertFalse(p.test(10));
        }

        @Test
        public void shouldTakeFirstResultForGte() {
            final P p = P.gte(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(10));
            assertFalse(p.test(9));
        }

        @Test
        public void shouldTakeFirstResultForLte() {
            final P p = P.lte(__.union(__.constant(10), __.constant(20)).asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(10));
            assertFalse(p.test(11));
        }

        @Test
        public void shouldAcceptMultipleResultsForWithin() {
            final P p = P.within(__.inject(1, 2, 3).fold().asAdmin());
            p.resolve(createTraverser("start"));
            assertTrue(p.test(1));
            assertTrue(p.test(2));
            assertTrue(p.test(3));
            assertFalse(p.test(4));
        }

        @Test
        public void shouldAcceptMultipleResultsForWithout() {
            final P p = P.without(__.inject(1, 2, 3).fold().asAdmin());
            p.resolve(createTraverser("start"));
            assertFalse(p.test(1));
            assertFalse(p.test(2));
            assertFalse(p.test(3));
            assertTrue(p.test(4));
        }

        @Test
        public void shouldPassWithoutWhenTraversalResolvesEmpty() {
            final P p = P.without(__.limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertFalse(p.isResolvedEmpty());
            assertTrue(p.test("anything"));
        }

        @Test
        public void shouldFailWithinWhenTraversalResolvesEmpty() {
            final P p = P.within(__.limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertFalse(p.isResolvedEmpty());
            assertFalse(p.test("anything"));
        }

        @Test
        public void shouldRemainResolvedEmptyForScalarPredicateWithEmptyTraversal() {
            final P p = P.eq(__.limit(0).asAdmin());
            p.resolve(createTraverser("anything"));
            assertTrue(p.isResolvedEmpty());
        }
    }

    /**
     * Tests for ConnectiveP (and/or) and NotP with traversal-bearing operands, including
     * deeply nested combinations and empty-resolution edge cases.
     */
    public static class TraversalConnectiveTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        @Test
        public void shouldResolveAndPWithTraversalOperands() {
            final P p = P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(20).asAdmin()));
            p.resolve(createTraverser("start"));
            assertTrue(p.test(15));
            assertFalse(p.test(10));
            assertFalse(p.test(25));
        }

        @Test
        public void shouldShortCircuitAndPResolveWhenScalarChildEmpty() {
            final P p = P.eq(__.limit(0).asAdmin()).and(P.gt(__.constant(5).asAdmin()));
            p.resolve(createTraverser("start"));
            assertTrue(p.isResolvedEmpty());
        }

        @Test
        public void shouldResolveOrPWithTraversalOperands() {
            final P p = P.eq(__.constant(1).asAdmin()).or(P.eq(__.constant(2).asAdmin()));
            p.resolve(createTraverser("start"));
            assertTrue(p.test(1));
            assertTrue(p.test(2));
            assertFalse(p.test(3));
        }

        @Test
        public void shouldResolveNestedAndInsideOr() {
            final P p = P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(20).asAdmin()))
                     .or(P.gt(__.constant(50).asAdmin()).and(P.lt(__.constant(60).asAdmin())));
            p.resolve(createTraverser("x"));
            assertTrue(p.test(15));
            assertTrue(p.test(55));
            assertFalse(p.test(25));
            assertFalse(p.test(5));
        }

        @Test
        public void shouldResolveNestedOrInsideAnd() {
            final P p = P.eq(__.constant(1).asAdmin()).or(P.eq(__.constant(2).asAdmin()))
                     .and(P.eq(__.constant(2).asAdmin()).or(P.eq(__.constant(3).asAdmin())));
            p.resolve(createTraverser("x"));
            assertTrue(p.test(2));
            assertFalse(p.test(1));
            assertFalse(p.test(3));
        }

        @Test
        public void shouldResolveDeeplyNestedConnectives() {
            final P p = P.gt(__.constant(0).asAdmin()).and(P.lt(__.constant(10).asAdmin()))
                     .or(P.gt(__.constant(20).asAdmin())
                          .and(P.lt(__.constant(30).asAdmin()).or(P.gt(__.constant(90).asAdmin()))));
            p.resolve(createTraverser("x"));
            assertTrue(p.test(5));
            assertTrue(p.test(25));
            assertTrue(p.test(95));
            assertFalse(p.test(15));
            assertFalse(p.test(50));
        }

        @Test
        public void shouldResolveOrWithOneEmptyChild() {
            final P p = P.eq(__.limit(0).asAdmin()).or(P.eq(__.constant(42).asAdmin()));
            p.resolve(createTraverser("x"));
            assertTrue(p.test(42));
            assertFalse(p.test(99));
        }

        @Test
        public void shouldResolveNotPWrappingTraversalPredicate() {
            final P p = P.eq(__.constant(42).asAdmin()).negate();
            p.resolve(createTraverser("start"));
            assertFalse(p.test(42));
            assertTrue(p.test(99));
        }

        @Test
        public void shouldProduceConsistentResultsAcrossManySequentialResolves() {
            final P p = P.gt(__.constant(10).asAdmin());
            for (int i = 0; i < 1000; i++) {
                p.resolve(createTraverser("start" + i));
                assertTrue(p.test(11));
                assertFalse(p.test(5));
            }
        }
    }

    /**
     * Tests for clone() and negate() operations on traversal-bearing predicates.
     */
    public static class TraversalCloneAndNegateTest {

        private Traverser.Admin<?> createTraverser(final Object value) {
            return new B_O_Traverser<>(value, 1L);
        }

        @Test
        public void shouldCloneScalarTraversalPredicate() {
            final P original = P.gt(__.constant(10).asAdmin());
            final P clone = original.clone();
            assertTrue(clone.hasTraversal());
            clone.resolve(createTraverser("x"));
            assertTrue(clone.test(15));
            assertFalse(clone.test(5));
        }

        @Test
        public void shouldCloneWithinTraversalPredicate() {
            final P original = P.within(__.inject(1, 2, 3).fold().asAdmin());
            final P clone = original.clone();
            assertTrue(clone.hasTraversal());
            clone.resolve(createTraverser("x"));
            assertTrue(clone.test(2));
            assertFalse(clone.test(9));
        }

        @Test
        public void shouldCloneConnectivePWithTraversals() {
            final P original = P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(20).asAdmin()));
            final P clone = original.clone();
            assertTrue(clone.hasTraversal());
            clone.resolve(createTraverser("x"));
            assertTrue(clone.test(15));
            assertFalse(clone.test(25));
        }

        @Test
        public void shouldCloneIndependentlyFromOriginal() {
            final P original = P.eq(__.constant(42).asAdmin());
            final P clone = original.clone();
            clone.resolve(createTraverser("x"));
            assertTrue(clone.test(42));
            assertTrue(original.hasTraversal());
        }

        @Test
        public void shouldNegateScalarTraversalPredicate() {
            final P p = P.gt(__.constant(10).asAdmin()).negate();
            assertTrue(p.hasTraversal());
            p.resolve(createTraverser("x"));
            assertTrue(p.test(5));
            assertFalse(p.test(15));
        }

        @Test
        public void shouldNegateConnectivePWithTraversals() {
            final P p = P.gt(__.constant(10).asAdmin()).and(P.lt(__.constant(20).asAdmin())).negate();
            p.resolve(createTraverser("x"));
            assertTrue(p.test(5));
            assertTrue(p.test(25));
            assertFalse(p.test(15));
        }

        @Test
        public void shouldCloneThenNegate() {
            final P original = P.eq(__.constant(42).asAdmin());
            final P negated = original.clone().negate();
            assertTrue(negated.hasTraversal());
            negated.resolve(createTraverser("x"));
            assertFalse(negated.test(42));
            assertTrue(negated.test(99));
        }

        @Test
        public void shouldNegateMultiTraversalWithin() {
            final P p = P.within(__.inject(1, 2, 3).fold().asAdmin()).negate();
            assertTrue(p.hasTraversal());
            p.resolve(createTraverser("x"));
            assertFalse(p.test(1));
            assertTrue(p.test(9));
        }
    }
}
