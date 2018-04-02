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

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class OperatorTest {

    /**
     * Required to verify that Operator can handle Number type, that it doesn't know explicitly.
     */
    static class CustomNumber extends Number implements Comparable<CustomNumber> {

        public final static CustomNumber ONE = new CustomNumber(1);
        public final static CustomNumber TEN = new CustomNumber(10);

        private int n;

        private CustomNumber(final int number) {
            this.n = number;
        }

        @Override
        public int intValue() {
            return n;
        }

        @Override
        public long longValue() {
            return n;
        }

        @Override
        public float floatValue() {
            return n;
        }

        @Override
        public double doubleValue() {
            return n;
        }

        @Override
        public int compareTo(final CustomNumber anotherCustomNumber) {
            return Integer.compare(n, anotherCustomNumber.n);
        }
    }

    @Parameterized.Parameters(name = "{0}({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        return new ArrayList<>(Arrays.asList(new Object[][]{
                {Operator.div, 10, 2, 5},
                {Operator.div, 10l, 2l, 5l},
                {Operator.div, 10f, 2f, 5f},
                {Operator.div, 10d, 2d, 5d},
                {Operator.div, 10, 2d, 5d},
                {Operator.div, 10, 2f, 5f},
                {Operator.div, 10d, 2, 5d},
                {Operator.div, 10f, 2d, 5d},
                {Operator.div, 10f, 2l, 5d},
                {Operator.div, 10f, 2, 5f},
                {Operator.div, BigInteger.TEN, 2, BigInteger.valueOf(5l)},
                {Operator.div, BigInteger.TEN, 2f, BigDecimal.valueOf(5l)},
                {Operator.div, BigDecimal.TEN, 2, BigDecimal.valueOf(5l)},
                {Operator.div, CustomNumber.TEN, 2, BigDecimal.valueOf(5l)},
                {Operator.max, 10, 2, 10},
                {Operator.max, 10l, 2l, 10l},
                {Operator.max, 10f, 2f, 10f},
                {Operator.max, 10d, 2d, 10d},
                {Operator.max, 2, 10, 10},
                {Operator.max, 2l, 10l, 10l},
                {Operator.max, 2f, 10f, 10f},
                {Operator.max, 2d, 10d, 10d},
                {Operator.max, 10, 2d, 10d},
                {Operator.max, 10, 2f, 10f},
                {Operator.max, 10d, 2, 10d},
                {Operator.max, 10f, 2d, 10d},
                {Operator.max, 10f, 2l, 10d},
                {Operator.max, 10f, 2, 10f},
                {Operator.max, BigInteger.TEN, 1, BigInteger.TEN},
                {Operator.max, BigInteger.TEN, BigDecimal.ONE, BigDecimal.TEN},
                {Operator.max, BigDecimal.TEN, 1, BigDecimal.TEN},
                {Operator.max, 1, CustomNumber.TEN, BigDecimal.TEN},
                {Operator.min, 10, 2, 2},
                {Operator.min, 10l, 2l, 2l},
                {Operator.min, 10f, 2f, 2f},
                {Operator.min, 10d, 2d, 2d},
                {Operator.min, 2, 10, 2},
                {Operator.min, 2l, 10l, 2l},
                {Operator.min, 2f, 10f, 2f},
                {Operator.min, 2d, 10d, 2d},
                {Operator.min, 10, 2d, 2d},
                {Operator.min, 10, 2f, 2f},
                {Operator.min, 10d, 2, 2d},
                {Operator.min, 10f, 2d, 2d},
                {Operator.min, 10f, 2l, 2d},
                {Operator.min, 10f, 2, 2f},
                {Operator.min, BigInteger.TEN, 1, BigInteger.ONE},
                {Operator.min, BigInteger.TEN, BigDecimal.ONE, BigDecimal.ONE},
                {Operator.min, BigDecimal.TEN, 1, BigDecimal.ONE},
                {Operator.min, 1, CustomNumber.TEN, BigDecimal.ONE},
                {Operator.minus, 10, 2, 8},
                {Operator.minus, 10l, 2l, 8l},
                {Operator.minus, 10f, 2f, 8f},
                {Operator.minus, 10d, 2d, 8d},
                {Operator.minus, 10, 2d, 8d},
                {Operator.minus, 10, 2f, 8f},
                {Operator.minus, 10d, 2, 8d},
                {Operator.minus, 10f, 2d, 8d},
                {Operator.minus, 10f, 2l, 8d},
                {Operator.minus, 10f, 2, 8f},
                {Operator.minus, BigInteger.TEN, 2, BigInteger.valueOf(8l)},
                {Operator.minus, BigInteger.TEN, 2f, BigDecimal.valueOf(8d)},
                {Operator.minus, BigDecimal.TEN, 2, BigDecimal.valueOf(8l)},
                {Operator.minus, CustomNumber.TEN, 2, BigDecimal.valueOf(8l)},
                {Operator.mult, 5, 4, 20},
                {Operator.mult, 5l, 4l, 20l},
                {Operator.mult, 5f, 4f, 20f},
                {Operator.mult, 5d, 4d, 20d},
                {Operator.mult, 5, 4d, 20d},
                {Operator.mult, 5, 4f, 20f},
                {Operator.mult, 5d, 4, 20d},
                {Operator.mult, 5f, 4d, 20d},
                {Operator.mult, 5f, 4l, 20d},
                {Operator.mult, 5f, 4, 20f},
                {Operator.mult, BigInteger.valueOf(5l), 4, BigInteger.valueOf(20l)},
                {Operator.mult, BigInteger.valueOf(5l), 4f, BigDecimal.valueOf(20d)},
                {Operator.mult, BigDecimal.valueOf(5d), 4, BigDecimal.valueOf(20d)},
                {Operator.mult, CustomNumber.TEN, 2f, BigDecimal.valueOf(20d)},
                {Operator.sum, 7, 3, 10},
                {Operator.sum, 7l, 3l, 10l},
                {Operator.sum, 7f, 3f, 10f},
                {Operator.sum, 7d, 3d, 10d},
                {Operator.sum, 7, 3d, 10d},
                {Operator.sum, 7, 3f, 10f},
                {Operator.sum, 7d, 3, 10d},
                {Operator.sum, 7f, 3d, 10d},
                {Operator.sum, 7f, 3l, 10d},
                {Operator.sum, 7f, 3, 10f},
                {Operator.sum, BigInteger.valueOf(7l), 3, BigInteger.TEN},
                {Operator.sum, BigInteger.valueOf(7l), 3f, BigDecimal.valueOf(10d)},
                {Operator.sum, BigDecimal.valueOf(7d), 3, BigDecimal.valueOf(10d)},
                {Operator.sum, CustomNumber.TEN, CustomNumber.ONE, BigDecimal.valueOf(11l)}
        }));
    }

    @Parameterized.Parameter(value = 0)
    public Operator operator;

    @Parameterized.Parameter(value = 1)
    public Number a;

    @Parameterized.Parameter(value = 2)
    public Number b;

    @Parameterized.Parameter(value = 3)
    public Number expected;

    @Test
    public void shouldApplyOperator() {
        assertEquals(expected, operator.apply(a, b));
    }
}
