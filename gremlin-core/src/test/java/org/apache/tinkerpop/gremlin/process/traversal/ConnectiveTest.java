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

import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author Mike Personick (http://github.com/mikepersonick)
 */
@RunWith(Enclosed.class)
public class ConnectiveTest {

    @Test
    public void shouldUpdateVariablesInOr() {
        // P.gt and P.lt have GValue overloads enabling variables
        final P<Integer> p1 = P.gt(GValue.of("x", 0));
        final P<Integer> p2 = P.lt(GValue.of("y", 10));
        final OrP<Integer> or = new OrP<>(Arrays.asList(p1, p2));

        // Initially variables x=0, y=10
        // For value 5 -> gt(0) true OR lt(10) true => true
        assertEquals(true, or.test(5));

        // Update only x so that gt(7) for value 5 is false, but lt(10) still true => overall true
        or.updateVariable("x", 7);
        assertEquals(true, or.test(5));

        // Update y so that lt(4) for value 5 is false; now both false => overall false
        or.updateVariable("y", 4);
        assertEquals(false, or.test(5));
    }

    @Test
    public void shouldUpdateVariablesInAndWithNestedConnectives() {
        // Build nested ((gt(x) AND lt(y)) AND gt(z))
        final P<Integer> gtX = P.gt(GValue.of("x", 0));
        final P<Integer> ltY = P.lt(GValue.of("y", 10));
        final P<Integer> gtZ = P.gt(GValue.of("z", -1));
        final AndP<Integer> inner = new AndP<>(Arrays.asList(gtX, ltY));
        final AndP<Integer> and = new AndP<>(Arrays.asList(inner, gtZ));

        // value 5 should satisfy defaults
        assertEquals(true, and.test(5));

        // change x so gt(6) on 5 becomes false -> whole AND false
        and.updateVariable("x", 6);
        assertEquals(false, and.test(5));

        // fix x back and then make y tighter so lt(4) on 5 is false
        and.updateVariable("x", 0);
        and.updateVariable("y", 4);
        assertEquals(false, and.test(5));

        // relax y and tighten z so gt(6) on 5 false
        and.updateVariable("y", 10);
        and.updateVariable("z", 6);
        assertEquals(false, and.test(5));

        // final: set z to -1 so condition holds again
        and.updateVariable("z", -1);
        assertEquals(true, and.test(5));
    }

    @Test
    public void shouldNotAffectUnknownVariables() {
        final P<Integer> p1 = P.gt(GValue.of("known", 3));
        final P<Integer> p2 = P.lt(GValue.of("also", 9));
        final AndP<Integer> and = new AndP<>(Arrays.asList(p1, p2));

        // Update a name that does not exist should have no effect
        and.updateVariable("unknown", 100);
        assertEquals(true, and.test(5));
    }

    private static final Object VAL = 1;
    private static final P TRUE = P.eq(1);
    private static final P FALSE = P.gt(1);
    private static final P ERROR = P.lt(Double.NaN);

    @RunWith(Parameterized.class)
    public static class OrTest {
        @Rule
        public ExpectedException exceptionRule = ExpectedException.none();

        @Parameterized.Parameters(name = "Or.test({0},{1}) = {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {TRUE, TRUE, true},
                    {TRUE, FALSE, true},
                    {TRUE, ERROR, true},
                    {FALSE, TRUE, true},
                    {FALSE, FALSE, false},
                    {FALSE, ERROR, GremlinTypeErrorException.class},
                    {ERROR, TRUE, true},
                    {ERROR, FALSE, GremlinTypeErrorException.class},
                    {ERROR, ERROR, GremlinTypeErrorException.class},
            });
        }

        @Parameterized.Parameter(value = 0)
        public P first;

        @Parameterized.Parameter(value = 1)
        public P second;

        @Parameterized.Parameter(value = 2)
        public Object expected;

        @Test
        public void shouldTest() {
            if (expected instanceof Class)
                exceptionRule.expect((Class) expected);

            assertEquals(expected, new OrP(Arrays.asList(first,second)).test(VAL));
        }
    }

    @RunWith(Parameterized.class)
    public static class AndTest {
        @Rule
        public ExpectedException exceptionRule = ExpectedException.none();

        @Parameterized.Parameters(name = "And.test({0},{1}) = {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {TRUE, TRUE, true},
                    {TRUE, FALSE, false},
                    {TRUE, ERROR, GremlinTypeErrorException.class},
                    {FALSE, TRUE, false},
                    {FALSE, FALSE, false},
                    {FALSE, ERROR, false},
                    {ERROR, TRUE, GremlinTypeErrorException.class},
                    {ERROR, FALSE, false},
                    {ERROR, ERROR, GremlinTypeErrorException.class},
            });
        }

        @Parameterized.Parameter(value = 0)
        public P first;

        @Parameterized.Parameter(value = 1)
        public P second;

        @Parameterized.Parameter(value = 2)
        public Object expected;

        @Test
        public void shouldTest() {
            if (expected instanceof Class)
                exceptionRule.expect((Class) expected);

            assertEquals(expected, new AndP(Arrays.asList(first,second)).test(VAL));
        }
    }

    @RunWith(Parameterized.class)
    public static class XorTest {
        @Rule
        public ExpectedException exceptionRule = ExpectedException.none();

        @Parameterized.Parameters(name = "Xor.test({0},{1}) = {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {TRUE, TRUE, false},
                    {TRUE, FALSE, true},
                    {TRUE, ERROR, GremlinTypeErrorException.class},
                    {FALSE, TRUE, true},
                    {FALSE, FALSE, false},
                    {FALSE, ERROR, GremlinTypeErrorException.class},
                    {ERROR, TRUE, GremlinTypeErrorException.class},
                    {ERROR, FALSE, GremlinTypeErrorException.class},
                    {ERROR, ERROR, GremlinTypeErrorException.class},
            });
        }

        @Parameterized.Parameter(value = 0)
        public P first;

        @Parameterized.Parameter(value = 1)
        public P second;

        @Parameterized.Parameter(value = 2)
        public Object expected;

        @Test
        public void shouldTest() {
            if (expected instanceof Class)
                exceptionRule.expect((Class) expected);

            final P xor = new OrP(Arrays.asList(new AndP(Arrays.asList(first,second.negate())),
                                                new AndP(Arrays.asList(first.negate(),second))));

            assertEquals(expected, xor.test(VAL));
        }
    }
}
