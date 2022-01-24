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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
@RunWith(Parameterized.class)
public class CompareTest {

    @Parameterized.Parameters(name = "{0}.test({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        final List<Object[]> testCases = new ArrayList<>(Arrays.asList(new Object[][]{
                {Compare.eq, null, null, true},
                {Compare.eq, null, 1, false},
                {Compare.eq, 1, null, false},
                {Compare.eq, "1", "1", true},
                {Compare.eq, 100, 99, false},
                {Compare.eq, 100, 101, false},
                {Compare.eq, "z", "a", false},
                {Compare.eq, "a", "z", false},
                {Compare.eq, new Object(), new Object(), false},
                {Compare.neq, null, null, false},
                {Compare.neq, null, 1, true},
                {Compare.neq, 1, null, true},
                {Compare.neq, "1", "1", false},
                {Compare.neq, 100, 99, true},
                {Compare.neq, 100, 101, true},
                {Compare.neq, "z", "a", true},
                {Compare.neq, "a", "z", true},
                {Compare.neq, new Object(), new Object(), true},
                {Compare.gt, null, null, false},
                {Compare.gt, null, 1, false},
                {Compare.gt, 1, null, false},
                {Compare.gt, "1", "1", false},
                {Compare.gt, 100, 99, true},
                {Compare.gt, 100, 101, false},
                {Compare.gt, "z", "a", true},
                {Compare.gt, "a", "z", false},
                {Compare.lt, null, null, false},
                {Compare.lt, null, 1, false},
                {Compare.lt, 1, null, false},
                {Compare.lt, "1", "1", false},
                {Compare.lt, 100, 99, false},
                {Compare.lt, 100, 101, true},
                {Compare.lt, "z", "a", false},
                {Compare.lt, "a", "z", true},
                {Compare.gte, null, null, true},
                {Compare.gte, null, 1, false},
                {Compare.gte, 1, null, false},
                {Compare.gte, "1", "1", true},
                {Compare.gte, 100, 99, true},
                {Compare.gte, 100, 101, false},
                {Compare.gte, "z", "a", true},
                {Compare.gte, "a", "z", false},
                {Compare.lte, null, null, true},
                {Compare.lte, null, 1, false},
                {Compare.lte, 1, null, false},
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
                {Compare.lte, new D(), new D(), true}
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
//
//        // Compare bigdecimal numbers with float infinite
//        final Float floatNegativeInfinite = Float.NEGATIVE_INFINITY;
//        testCases.addAll(Arrays.asList(new Object[][]{
//            // big500 - floatNegativeInfinite
//            {Compare.eq, big500, floatNegativeInfinite, false},
//            {Compare.neq, big500, floatNegativeInfinite, true},
//            {Compare.gt, big500, floatNegativeInfinite, true},
//            {Compare.lt, big500, floatNegativeInfinite, false},
//            {Compare.gte, big500, floatNegativeInfinite, true},
//            {Compare.lte, big500, floatNegativeInfinite, false},
//            // floatNegativeInfinite - big500
//            {Compare.eq, floatNegativeInfinite, big500, false},
//            {Compare.neq, floatNegativeInfinite, big500, true},
//            {Compare.gt, floatNegativeInfinite, big500, false},
//            {Compare.lt, floatNegativeInfinite, big500, true},
//            {Compare.gte, floatNegativeInfinite, big500, false},
//            {Compare.lte, floatNegativeInfinite, big500, true},
//        }));

        return testCases;
    }

    @Parameterized.Parameter(value = 0)
    public Compare compare;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Object second;

    @Parameterized.Parameter(value = 3)
    public boolean expected;

    @Test
    public void shouldTest() {
        assertEquals(expected, compare.test(first, second));
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
