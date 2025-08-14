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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
                    {P.eq(null), null, true},
                    {P.eq(null), 0, false},
                    {P.eq(Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, false},
                    {P.eq(Float.POSITIVE_INFINITY), Float.NEGATIVE_INFINITY, false},
                    {P.eq(Float.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, false},
                    {P.neq(0), 0, false},
                    {P.neq(0), -0, false},
                    {P.neq(0), +0, false},
                    {P.neq(-0), +0, false},
                    {P.neq(0), 1, true},
                    {P.neq(0), null, true},
                    {P.neq(null), null, false},
                    {P.neq(null), 0, true},
                    {P.neq(Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, true},
                    {P.neq(Float.POSITIVE_INFINITY), Float.NEGATIVE_INFINITY, true},
                    {P.neq(Float.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY, true},
                    {P.gt(0), -1, false},
                    {P.gt(0), 0, false},
                    {P.gt(0), 1, true},
                    {P.lt(0), -1, true},
                    {P.lt(0), 0, false},
                    {P.lt(0), 1, false},
                    {P.gte(0), -1, false},
                    {P.gte(0), 0, true},
                    {P.gte(0), 1, true},
                    {P.lte(0), -1, true},
                    {P.lte(0), 0, true},
                    {P.lte(0), 1, false},
                    {P.between(1, 10), 0, false},
                    {P.between(1, 10), 1, true},
                    {P.between(1, 10), 9, true},
                    {P.between(1, 10), 10, false},
                    {P.inside(1, 10), 0, false},
                    {P.inside(1, 10), 1, false},
                    {P.inside(1, 10), 9, true},
                    {P.inside(1, 10), 10, false},
                    {P.outside(1, 10), 0, true},
                    {P.outside(1, 10), 1, false},
                    {P.outside(1, 10), 9, false},
                    {P.outside(1, 10), 10, false},
                    {P.outside(1, Double.NaN), 0, true},
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
                    {P.between("m", "n").and(P.neq("marko")), "marko", false},
                    {P.between("m", "n").and(P.neq("marko")), "matthias", true},
                    {P.between("m", "n").or(P.eq("daniel")), "marko", true},
                    {P.between("m", "n").or(P.eq("daniel")), "daniel", true},
                    {P.between("m", "n").or(P.eq("daniel")), "stephen", false},
                    {P.within().and(P.within()), 0, false},
                    {P.within().and(P.without()), 0, false},
                    {P.without().and(P.without()), 0, true},
                    {P.within().and(P.without()), 0, false},
                    {P.within().or(P.within()), 0, false},
                    {P.within().or(P.without()), 0, true},
                    {P.without().or(P.without()), 0, true},
                    // text predicates
                    {TextP.containing("ark"), "marko", true},
                    {TextP.containing("ark"), "josh", false},
                    {TextP.startingWith("jo"), "marko", false},
                    {TextP.startingWith("jo"), "josh", true},
                    {TextP.endingWith("ter"), "marko", false},
                    {TextP.endingWith("ter"), "peter", true},
                    {TextP.containing("o"), "marko", true},
                    {TextP.containing("o"), "josh", true},
                    {TextP.containing("o").and(P.gte("j")), "marko", true},
                    {TextP.containing("o").and(P.gte("j")), "josh", true},
                    {TextP.containing("o").and(P.gte("j")).and(TextP.endingWith("ko")), "marko", true},
                    {TextP.containing("o").and(P.gte("j")).and(TextP.endingWith("ko")), "josh", false},
                    {TextP.containing("o").and(P.gte("j").and(TextP.endingWith("ko"))), "marko", true},
                    {TextP.containing("o").and(P.gte("j").and(TextP.endingWith("ko"))), "josh", false},

                    // type errors
                    {P.outside(Double.NaN, Double.NaN), 0, GremlinTypeErrorException.class},
                    {P.inside(-1, Double.NaN), 0, GremlinTypeErrorException.class},
                    {P.inside(Double.NaN, 1), 0, GremlinTypeErrorException.class},
                    {TextP.containing(null), "abc", GremlinTypeErrorException.class},
                    {TextP.containing("abc"), null, GremlinTypeErrorException.class},
                    {TextP.containing(null), null, GremlinTypeErrorException.class},
                    {TextP.startingWith(null), "abc", GremlinTypeErrorException.class},
                    {TextP.startingWith("abc"), null, GremlinTypeErrorException.class},
                    {TextP.startingWith(null), null, GremlinTypeErrorException.class},
                    {TextP.endingWith(null), "abc", GremlinTypeErrorException.class},
                    {TextP.endingWith("abc"), null, GremlinTypeErrorException.class},
                    {TextP.endingWith(null), null, GremlinTypeErrorException.class},
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
        private static final String NEQ_FORMAT = "neq(%d)";
        public static final String GT_FORMAT = "gt(%d)";
        public static final String LT_FORMAT = "lt(%d)";

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
            assertEquals(String.format(NEQ_FORMAT, INITIAL_VALUE), negated.toString());
            
            predicate.setValue(UPDATED_VALUE);
            P<Integer> updatedNegated = predicate.negate();
            assertTrue(updatedNegated.test(INITIAL_VALUE));
            assertFalse(updatedNegated.test(UPDATED_VALUE));
            assertEquals(String.format(NEQ_FORMAT, UPDATED_VALUE), updatedNegated.toString());
        }

        @Test
        public void shouldHandleNullValuesForSetValue() {
            P<Integer> predicate = P.eq(INITIAL_VALUE);
            predicate.setValue(null);
            
            assertTrue(predicate.test(null));
            assertFalse(predicate.test(INITIAL_VALUE));
            assertEquals("eq", predicate.toString());
            assertEquals(predicate, P.eq(null));
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
}
