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

import org.apache.tinkerpop.gremlin.util.tools.CollectionFactory;
import org.javatuples.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asList;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asSet;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
@RunWith(Parameterized.class)
public class CompareTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private static final Object NaN = new Object() {
        public String toString() { return "NaN"; }
    };

    @Parameterized.Parameters(name = "{0}.test({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        final List<Object[]> testCases = new ArrayList<>(Arrays.asList(new Object[][]{
                {Compare.eq, "1", "1", true},
                {Compare.eq, 100, 99, false},
                {Compare.eq, 100, 101, false},
                {Compare.eq, "z", "a", false},
                {Compare.eq, "a", "z", false},
                {Compare.eq, new Object(), new Object(), false},
                {Compare.neq, "1", "1", false},
                {Compare.neq, 100, 99, true},
                {Compare.neq, 100, 101, true},
                {Compare.neq, "z", "a", true},
                {Compare.neq, "a", "z", true},
                {Compare.neq, new Object(), new Object(), true},
                {Compare.gt, "1", "1", false},
                {Compare.gt, 100, 99, true},
                {Compare.gt, 100, 101, false},
                {Compare.gt, "z", "a", true},
                {Compare.gt, "a", "z", false},
                {Compare.lt, "1", "1", false},
                {Compare.lt, 100, 99, false},
                {Compare.lt, 100, 101, true},
                {Compare.lt, "z", "a", false},
                {Compare.lt, "a", "z", true},
                {Compare.gte, "1", "1", true},
                {Compare.gte, 100, 99, true},
                {Compare.gte, 100, 101, false},
                {Compare.gte, "z", "a", true},
                {Compare.gte, "a", "z", false},
                {Compare.lte, "1", "1", true},
                {Compare.lte, 100, 99, false},
                {Compare.lte, 100, 101, true},
                {Compare.lte, "z", "a", false},
                {Compare.lte, "a", "z", true},
                {Compare.lte, new A(), new A(), true},
                {Compare.lte, new B(), new B(), true},
                {Compare.gte, new B(), new C(), true},
                {Compare.lte, new B(), new D(), true},
                {Compare.lte, new C(), new C(), true},
                {Compare.lte, new D(), new D(), true},

                // type promotion
                {Compare.eq,  1, 1.0d, true},
                {Compare.neq, 1, 1.0d, false},
                {Compare.lt,  1, 1.0d, false},
                {Compare.lte, 1, 1.0d, true},
                {Compare.gt,  1, 1.0d, false},
                {Compare.gte, 1, 1.0d, true},

                // Incomparable types produce ERROR
                {Compare.gt,  23, "23", GremlinTypeErrorException.class},
                {Compare.gte, 23, "23", GremlinTypeErrorException.class},
                {Compare.lt,  23, "23", GremlinTypeErrorException.class},
                {Compare.lte, 23, "23", GremlinTypeErrorException.class},
                {Compare.lte, new CompareTest.A(), new CompareTest.B(), GremlinTypeErrorException.class},
                {Compare.lte, new CompareTest.B(), new CompareTest.A(), GremlinTypeErrorException.class},
                {Compare.lte, new CompareTest.C(), new CompareTest.D(), GremlinTypeErrorException.class},
                {Compare.gte, new Object(), new Object(), GremlinTypeErrorException.class},
                {Compare.gte, new Object(), new Object(), GremlinTypeErrorException.class},
                {Compare.gte, new Object(), new Object(), GremlinTypeErrorException.class},

                /*
                 * NaN has pretty much the same comparability behavior against any argument (including itself):
                 * P.eq(NaN, any) = FALSE
                 * P.neq(NaN, any) = TRUE
                 * P.lt/lte/gt/gte(NaN, any) = ERROR -> FALSE
                 */
                {Compare.eq,  NaN, NaN, false},
                {Compare.neq, NaN, NaN, true},
                {Compare.gt,  NaN, NaN, GremlinTypeErrorException.class},
                {Compare.gte, NaN, NaN, GremlinTypeErrorException.class},
                {Compare.lt,  NaN, NaN, GremlinTypeErrorException.class},
                {Compare.lte, NaN, NaN, GremlinTypeErrorException.class},

                {Compare.eq,  NaN, 0, false},
                {Compare.neq, NaN, 0, true},
                {Compare.gt,  NaN, 0, GremlinTypeErrorException.class},
                {Compare.gte, NaN, 0, GremlinTypeErrorException.class},
                {Compare.lt,  NaN, 0, GremlinTypeErrorException.class},
                {Compare.lte, NaN, 0, GremlinTypeErrorException.class},

                {Compare.eq,  NaN, "foo", false},
                {Compare.neq, NaN, "foo", true},
                {Compare.gt,  NaN, "foo", GremlinTypeErrorException.class},
                {Compare.gte, NaN, "foo", GremlinTypeErrorException.class},
                {Compare.lt,  NaN, "foo", GremlinTypeErrorException.class},
                {Compare.lte, NaN, "foo", GremlinTypeErrorException.class},

                /*
                 * We consider null to be in its own type space, and thus not comparable (lt/lte/gt/gte) with
                 * anything other than null:
                 *
                 * P.eq(null, any non-null) = FALSE
                 * P.neq(null, any non-null) = TRUE
                 * P.lt/lte/gt/gte(null, any non-null) = ERROR -> FALSE
                 */
                {Compare.eq,  null, null, true},
                {Compare.neq, null, null, false},
                {Compare.gt,  null, null, false},
                {Compare.gte, null, null, true},
                {Compare.lt,  null, null, false},
                {Compare.lte, null, null, true},

                {Compare.eq,  "foo", null, false},
                {Compare.neq, "foo", null, true},
                {Compare.gt,  "foo", null, GremlinTypeErrorException.class},
                {Compare.gte, "foo", null, GremlinTypeErrorException.class},
                {Compare.lt,  "foo", null, GremlinTypeErrorException.class},
                {Compare.lte, "foo", null, GremlinTypeErrorException.class},

                {Compare.eq,  null, 1, false},
                {Compare.eq,  1, null, false},
                {Compare.neq, null, 1, true},
                {Compare.neq, 1, null, true},
                {Compare.gt,  null, 1, GremlinTypeErrorException.class},
                {Compare.gt,  1, null, GremlinTypeErrorException.class},
                {Compare.gte, null, 1, GremlinTypeErrorException.class},
                {Compare.gte, 1, null, GremlinTypeErrorException.class},
                {Compare.lt,  null, 1, GremlinTypeErrorException.class},
                {Compare.lt,  1, null, GremlinTypeErrorException.class},
                {Compare.lte, null, 1, GremlinTypeErrorException.class},
                {Compare.lte, 1, null, GremlinTypeErrorException.class},

                {Compare.eq,  NaN, null, false},
                {Compare.neq, NaN, null, true},
                {Compare.gt,  NaN, null, GremlinTypeErrorException.class},
                {Compare.gte, NaN, null, GremlinTypeErrorException.class},
                {Compare.lt,  NaN, null, GremlinTypeErrorException.class},
                {Compare.lte, NaN, null, GremlinTypeErrorException.class},

                /*
                 * Collections
                 */
                {Compare.eq, asList(0), asList(0), true},
                {Compare.neq, asList(0), asList(0), false},
                {Compare.lt, asList(0), asList(0), false},
                {Compare.lte, asList(0), asList(0), true},
                {Compare.gt, asList(0), asList(0), false},
                {Compare.gte, asList(0), asList(0), true},

                {Compare.eq, asList(0), asList(1), false},
                {Compare.neq, asList(0), asList(1), true},
                {Compare.lt, asList(0), asList(1), true},
                {Compare.lte, asList(0), asList(1), true},
                {Compare.gt, asList(0), asList(1), false},
                {Compare.gte, asList(0), asList(1), false},

                {Compare.eq, asList(1,2,3), asList(1,2,4), false},
                {Compare.neq, asList(1,2,3), asList(1,2,4), true},
                {Compare.lt, asList(1,2,3), asList(1,2,4), true},
                {Compare.lte, asList(1,2,3), asList(1,2,4), true},
                {Compare.gt, asList(1,2,3), asList(1,2,4), false},
                {Compare.gte, asList(1,2,3), asList(1,2,4), false},

                {Compare.eq, asList(Double.NaN), asList(Double.NaN), false},
                {Compare.neq, asList(Double.NaN), asList(Double.NaN), true},
                {Compare.lt, asList(Double.NaN), asList(Double.NaN), GremlinTypeErrorException.class},
                {Compare.lte, asList(Double.NaN), asList(Double.NaN), GremlinTypeErrorException.class},
                {Compare.gt, asList(Double.NaN), asList(Double.NaN), GremlinTypeErrorException.class},
                {Compare.gte, asList(Double.NaN), asList(Double.NaN), GremlinTypeErrorException.class},

                {Compare.eq, asList(Double.NaN), asList(0), false},
                {Compare.neq, asList(Double.NaN), asList(0), true},
                {Compare.lt, asList(Double.NaN), asList(0), GremlinTypeErrorException.class},
                {Compare.lte, asList(Double.NaN), asList(0), GremlinTypeErrorException.class},
                {Compare.gt, asList(Double.NaN), asList(0), GremlinTypeErrorException.class},
                {Compare.gte, asList(Double.NaN), asList(0), GremlinTypeErrorException.class},

                {Compare.eq, asMap(1, 1), asMap(1, null), false},
                {Compare.neq, asMap(1, 1), asMap(1, null), true},
                {Compare.lt, asMap(1, 1), asMap(1, null), GremlinTypeErrorException.class},
                {Compare.lte, asMap(1, 1), asMap(1, null), GremlinTypeErrorException.class},
                {Compare.gt, asMap(1, 1), asMap(1, null), GremlinTypeErrorException.class},
                {Compare.gte, asMap(1, 1), asMap(1, null), GremlinTypeErrorException.class},

                {Compare.eq, asList(0), asList("foo"), false},
                {Compare.neq, asList(0), asList("foo"), true},
                {Compare.lt, asList(0), asList("foo"), GremlinTypeErrorException.class},
                {Compare.lte, asList(0), asList("foo"), GremlinTypeErrorException.class},
                {Compare.gt, asList(0), asList("foo"), GremlinTypeErrorException.class},
                {Compare.gte, asList(0), asList("foo"), GremlinTypeErrorException.class},

                // sets are sorted
                {Compare.eq, asSet(1.0, 2.0), asSet(2, 1), true},
                {Compare.neq, asSet(1.0, 2.0), asSet(2, 1), false},
                {Compare.lt, asSet(1.0, 2.0), asSet(2, 1), false},
                {Compare.lte, asSet(1.0, 2.0), asSet(2, 1), true},
                {Compare.gt, asSet(1.0, 2.0), asSet(2, 1), false},
                {Compare.gte, asSet(1.0, 2.0), asSet(2, 1), true},

                // maps are sorted
                {Compare.eq, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), true},
                {Compare.neq, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), false},
                {Compare.lt, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), false},
                {Compare.lte, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), true},
                {Compare.gt, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), false},
                {Compare.gte, asMap(1.0, "foo", 2.0, "bar"), asMap(2, "bar", 1, "foo"), true},

        }));
        // Compare Numbers of mixed types.
        final List<Object> one = Arrays.asList(1, 1l, 1d, 1f, BigDecimal.ONE, BigInteger.ONE);
        for (Object i : one) {
            for (Object j : one) {
                testCases.addAll(Arrays.asList(new Object[][]{
                        {Compare.eq, i, j, true},
                        {Compare.neq, i, j, false},
                        {Compare.gt, i, j, false},
                        {Compare.lt, i, j, false},
                        {Compare.gte, i, j, true},
                        {Compare.lte, i, j, true},
                }));
            }
        }
        // Compare large numbers of different types that cannot convert to doubles losslessly.
        final BigInteger big1 = new BigInteger("123456789012345678901234567890");
        final BigDecimal big1d = new BigDecimal("123456789012345678901234567890");
        final BigDecimal big2 = new BigDecimal(big1.add(BigInteger.ONE));
        testCases.addAll(Arrays.asList(new Object[][]{
                // big1 == big1d
                {Compare.eq, big1, big1d, true},
                {Compare.neq, big1, big1d, false},
                {Compare.gt, big1, big1d, false},
                {Compare.lt, big1, big1d, false},
                {Compare.gte, big1, big1d, true},
                {Compare.lte, big1, big1d, true},
                // big1 < big2
                {Compare.eq, big1, big2, false},
                {Compare.neq, big1, big2, true},
                {Compare.gt, big1, big2, false},
                {Compare.lt, big1, big2, true},
                {Compare.gte, big1, big2, false},
                {Compare.lte, big1, big2, true},
                // Reverse the operands for symmetric test coverage (big2 > big1)
                {Compare.eq, big2, big1, false},
                {Compare.neq, big2, big1, true},
                {Compare.gt, big2, big1, true},
                {Compare.lt, big2, big1, false},
                {Compare.gte, big2, big1, true},
                {Compare.lte, big2, big1, false},
        }));

        // Compare bigdecimal numbers with double infinite
        final BigDecimal big500 = new BigDecimal("500.00");

        final Double doubleInfinite = Double.POSITIVE_INFINITY;
        testCases.addAll(Arrays.asList(new Object[][]{
            // big500 - doubleInfinite
            {Compare.eq, big500, doubleInfinite, false},
            {Compare.neq, big500, doubleInfinite, true},
            {Compare.gt, big500, doubleInfinite, false},
            {Compare.lt, big500, doubleInfinite, true},
            {Compare.gte, big500, doubleInfinite, false},
            {Compare.lte, big500, doubleInfinite, true},
            // doubleInfinite - big500
            {Compare.eq, doubleInfinite, big500, false},
            {Compare.neq, doubleInfinite, big500, true},
            {Compare.gt, doubleInfinite, big500, true},
            {Compare.lt, doubleInfinite, big500, false},
            {Compare.gte, doubleInfinite, big500, true},
            {Compare.lte, doubleInfinite, big500, false},
        }));

        // Compare bigdecimal numbers with float infinite
        final Float floatInfinite = Float.POSITIVE_INFINITY;
        testCases.addAll(Arrays.asList(new Object[][]{
            // big500 - floatInfinite
            {Compare.eq, big500, floatInfinite, false},
            {Compare.neq, big500, floatInfinite, true},
            {Compare.gt, big500, floatInfinite, false},
            {Compare.lt, big500, floatInfinite, true},
            {Compare.gte, big500, floatInfinite, false},
            {Compare.lte, big500, floatInfinite, true},
            // floatInfinite - big500
            {Compare.eq, floatInfinite, big500, false},
            {Compare.neq, floatInfinite, big500, true},
            {Compare.gt, floatInfinite, big500, true},
            {Compare.lt, floatInfinite, big500, false},
            {Compare.gte, floatInfinite, big500, true},
            {Compare.lte, floatInfinite, big500, false},
        }));

        final Double doubleNegativeInfinite = Double.NEGATIVE_INFINITY;
        testCases.addAll(Arrays.asList(new Object[][]{
            // big500 - doubleNegativeInfinite
            {Compare.eq, big500, doubleNegativeInfinite, false},
            {Compare.neq, big500, doubleNegativeInfinite, true},
            {Compare.gt, big500, doubleNegativeInfinite, true},
            {Compare.lt, big500, doubleNegativeInfinite, false},
            {Compare.gte, big500, doubleNegativeInfinite, true},
            {Compare.lte, big500, doubleNegativeInfinite, false},
            // doubleNegativeInfinite - big500
            {Compare.eq, doubleNegativeInfinite, big500, false},
            {Compare.neq, doubleNegativeInfinite, big500, true},
            {Compare.gt, doubleNegativeInfinite, big500, false},
            {Compare.lt, doubleNegativeInfinite, big500, true},
            {Compare.gte, doubleNegativeInfinite, big500, false},
            {Compare.lte, doubleNegativeInfinite, big500, true},
        }));

        // Compare bigdecimal numbers with float infinite
        final Float floatNegativeInfinite = Float.NEGATIVE_INFINITY;
        testCases.addAll(Arrays.asList(new Object[][]{
            // big500 - floatNegativeInfinite
            {Compare.eq, big500, floatNegativeInfinite, false},
            {Compare.neq, big500, floatNegativeInfinite, true},
            {Compare.gt, big500, floatNegativeInfinite, true},
            {Compare.lt, big500, floatNegativeInfinite, false},
            {Compare.gte, big500, floatNegativeInfinite, true},
            {Compare.lte, big500, floatNegativeInfinite, false},
            // floatNegativeInfinite - big500
            {Compare.eq, floatNegativeInfinite, big500, false},
            {Compare.neq, floatNegativeInfinite, big500, true},
            {Compare.gt, floatNegativeInfinite, big500, false},
            {Compare.lt, floatNegativeInfinite, big500, true},
            {Compare.gte, floatNegativeInfinite, big500, false},
            {Compare.lte, floatNegativeInfinite, big500, true},
        }));

        return testCases;
    }

    @Parameterized.Parameter(value = 0)
    public Compare compare;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Object second;

    @Parameterized.Parameter(value = 3)
    public Object expected;

    @Test
    public void shouldTest() {
        if (expected instanceof Class)
            exceptionRule.expect((Class) expected);

        if (first == NaN || second == NaN) {
            // test all the NaN combos
            final List<Pair> args = new ArrayList<>();
            if (first == NaN && second == NaN) {
                args.add(new Pair(Double.NaN, Double.NaN));
                args.add(new Pair(Double.NaN, Float.NaN));
                args.add(new Pair(Float.NaN, Double.NaN));
                args.add(new Pair(Float.NaN, Float.NaN));
            } else if (first == NaN) {
                args.add(new Pair(Double.NaN, second));
                args.add(new Pair(Float.NaN, second));
            } else {
                args.add(new Pair(first, Double.NaN));
                args.add(new Pair(first, Float.NaN));
            }
            for (Pair arg : args) {
                assertEquals(expected, compare.test(arg.getValue0(), arg.getValue1()));
            }
        } else {
            assertEquals(expected, compare.test(first, second));
        }
    }

    static class A implements Comparable<A> {

        @Override
        public int compareTo(A o) {
            return 0;
        }
    }

    static class B implements Comparable<B> {

        @Override
        public int compareTo(B o) {
            return 0;
        }
    }

    static class C extends B {
    }

    static class D extends B {
    }
}
