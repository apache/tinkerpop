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
package org.apache.tinkerpop.gremlin.util;

import org.javatuples.Quartet;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.add;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.compare;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.div;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.getHighestCommonNumberClass;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.max;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.min;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.mul;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.sub;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class NumberHelperTest {

    private final static List<Number> EACH_NUMBER_TYPE = Arrays.asList(
            (byte) 1, (short) 1, 1, 1L, 1F, 1D, BigInteger.ONE, BigDecimal.ONE
    );

    private final static List<Quartet<Number, Number, Class<? extends Number>, Class<? extends Number>>> COMMON_NUMBER_CLASSES =
            Arrays.asList(
                    // BYTE
                    new Quartet<>((byte) 1, (byte) 1, Byte.class, Float.class),
                    new Quartet<>((byte) 1, (short) 1, Short.class, Float.class),
                    new Quartet<>((byte) 1, 1, Integer.class, Float.class),
                    new Quartet<>((byte) 1, 1L, Long.class, Double.class),
                    new Quartet<>((byte) 1, 1F, Float.class, Float.class),
                    new Quartet<>((byte) 1, 1D, Double.class, Double.class),
                    new Quartet<>((byte) 1, BigInteger.ONE, BigInteger.class, BigDecimal.class),
                    new Quartet<>((byte) 1, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // SHORT
                    new Quartet<>((short) 1, (short) 1, Short.class, Float.class),
                    new Quartet<>((short) 1, 1, Integer.class, Float.class),
                    new Quartet<>((short) 1, 1L, Long.class, Double.class),
                    new Quartet<>((short) 1, 1F, Float.class, Float.class),
                    new Quartet<>((short) 1, 1D, Double.class, Double.class),
                    new Quartet<>((short) 1, BigInteger.ONE, BigInteger.class, BigDecimal.class),
                    new Quartet<>((short) 1, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // INTEGER
                    new Quartet<>(1, 1, Integer.class, Float.class),
                    new Quartet<>(1, 1L, Long.class, Double.class),
                    new Quartet<>(1, 1F, Float.class, Float.class),
                    new Quartet<>(1, 1D, Double.class, Double.class),
                    new Quartet<>(1, BigInteger.ONE, BigInteger.class, BigDecimal.class),
                    new Quartet<>(1, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // LONG
                    new Quartet<>(1L, 1L, Long.class, Double.class),
                    new Quartet<>(1L, 1F, Double.class, Double.class),
                    new Quartet<>(1L, 1D, Double.class, Double.class),
                    new Quartet<>(1L, BigInteger.ONE, BigInteger.class, BigDecimal.class),
                    new Quartet<>(1L, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // FLOAT
                    new Quartet<>(1F, 1F, Float.class, Float.class),
                    new Quartet<>(1F, 1D, Double.class, Double.class),
                    new Quartet<>(1F, BigInteger.ONE, BigDecimal.class, BigDecimal.class),
                    new Quartet<>(1F, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // DOUBLE
                    new Quartet<>(1D, 1D, Double.class, Double.class),
                    new Quartet<>(1D, BigInteger.ONE, BigDecimal.class, BigDecimal.class),
                    new Quartet<>(1D, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // BIG INTEGER
                    new Quartet<>(BigInteger.ONE, BigInteger.ONE, BigInteger.class, BigDecimal.class),
                    new Quartet<>(BigInteger.ONE, BigDecimal.ONE, BigDecimal.class, BigDecimal.class),
                    // BIG DECIMAL
                    new Quartet<>(BigDecimal.ONE, BigDecimal.ONE, BigDecimal.class, BigDecimal.class)
            );

    @Test
    public void shouldReturnHighestCommonNumberClass() {
        for (final Quartet<Number, Number, Class<? extends Number>, Class<? extends Number>> q : COMMON_NUMBER_CLASSES) {
            assertEquals(q.getValue2(), getHighestCommonNumberClass(q.getValue0(), q.getValue1()));
            assertEquals(q.getValue2(), getHighestCommonNumberClass(q.getValue1(), q.getValue0()));
            assertEquals(q.getValue3(), getHighestCommonNumberClass(true, q.getValue0(), q.getValue1()));
            assertEquals(q.getValue3(), getHighestCommonNumberClass(true, q.getValue1(), q.getValue0()));
        }
        // Double.NaN and null are not numbers and thus should be ignored
        for (final Number number : EACH_NUMBER_TYPE) {
            assertEquals(number.getClass(), getHighestCommonNumberClass(number, Double.NaN));
            assertEquals(number.getClass(), getHighestCommonNumberClass(Double.NaN, number));
            assertEquals(number.getClass(), getHighestCommonNumberClass(number, null));
            assertEquals(number.getClass(), getHighestCommonNumberClass(null, number));
        }
    }

    @Test
    public void shouldHandleAllNullInput() {
        assertNull(add(null, null));
        assertNull(sub(null, null));
        assertNull(mul(null, null));
        assertNull(div(null, null));
        assertNull(max((Number) null, null));
        assertNull(max((Comparable) null, null));
        assertNull(min((Number) null, null));
        assertNull(min((Comparable) null, null));
        assertEquals(0, compare(null, null).intValue());
    }

    @Test
    public void shouldAddAndReturnCorrectType() {
        assertEquals((byte) 1, add((byte) 1, (Byte) null));
        assertNull(add((Byte) null, (byte) 1));

        // BYTE
        assertEquals((byte) 2, add((byte) 1, (byte) 1));
        assertEquals((short) 2, add((byte) 1, (short) 1));
        assertEquals(2, add((byte) 1, 1));
        assertEquals(2L, add((byte) 1, 1L));
        assertEquals(2F, add((byte) 1, 1F));
        assertEquals(2D, add((byte) 1, 1D));
        assertEquals(BigInteger.ONE.add(BigInteger.ONE), add((byte) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add((byte) 1, BigDecimal.ONE));

        // SHORT
        assertEquals((short)2, add((short) 1, (short) 1));
        assertEquals(2, add((short) 1, 1));
        assertEquals(2L, add((short) 1, 1L));
        assertEquals(2F, add((short) 1, 1F));
        assertEquals(2D, add((short) 1, 1D));
        assertEquals(BigInteger.ONE.add(BigInteger.ONE), add((short) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add((short) 1, BigDecimal.ONE));

        // INTEGER
        assertEquals(2, add(1, 1));
        assertEquals(2L, add(1, 1L));
        assertEquals(2F, add(1, 1F));
        assertEquals(2D, add(1, 1D));
        assertEquals(BigInteger.ONE.add(BigInteger.ONE), add(1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add(1, BigDecimal.ONE));

        // LONG
        assertEquals(2L, add(1L, 1L));
        assertEquals(2D, add(1L, 1F));
        assertEquals(2D, add(1L, 1D));
        assertEquals(BigInteger.ONE.add(BigInteger.ONE), add(1L, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add(1L, BigDecimal.ONE));

        // FLOAT
        assertEquals(2F, add(1F, 1F));
        assertEquals(2D, add(1F, 1D));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE).setScale(1, RoundingMode.HALF_UP), add(1F, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE).setScale(1, RoundingMode.HALF_UP), add(1F, BigDecimal.ONE));

        // DOUBLE
        assertEquals(2D, add(1D, 1D));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE).setScale(1, RoundingMode.HALF_UP), add(1D, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE).setScale(1, RoundingMode.HALF_UP), add(1D, BigDecimal.ONE));

        // BIG INTEGER
        assertEquals(BigInteger.ONE.add(BigInteger.ONE), add(BigInteger.ONE, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add(BigInteger.ONE, BigDecimal.ONE));

        // BIG DECIMAL
        assertEquals(BigDecimal.ONE.add(BigDecimal.ONE), add(BigDecimal.ONE, BigDecimal.ONE));
    }

    @Test
    public void shouldSubtractAndReturnCorrectType() {
        assertEquals((byte) 1, sub((byte) 1, (Byte) null));
        assertNull(sub((Byte) null, (byte) 1));

        // BYTE
        assertEquals((byte) 0, sub((byte) 1, (byte) 1));
        assertEquals((short) 0, sub((byte) 1, (short) 1));
        assertEquals(0, sub((byte) 1, 1));
        assertEquals(0L, sub((byte) 1, 1L));
        assertEquals(0F, sub((byte) 1, 1F));
        assertEquals(0D, sub((byte) 1, 1D));
        assertEquals(BigInteger.ZERO, sub((byte) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO, sub((byte) 1, BigDecimal.ONE));

        // SHORT
        assertEquals((short) 0, sub((short) 1, (short) 1));
        assertEquals(0, sub((short) 1, 1));
        assertEquals(0L, sub((short) 1, 1L));
        assertEquals(0F, sub((short) 1, 1F));
        assertEquals(0D, sub((short) 1, 1D));
        assertEquals(BigInteger.ZERO, sub((short) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO, sub((short) 1, BigDecimal.ONE));

        // INTEGER
        assertEquals(0, sub(1, 1));
        assertEquals(0L, sub(1, 1L));
        assertEquals(0F, sub(1, 1F));
        assertEquals(0D, sub(1, 1D));
        assertEquals(BigInteger.ZERO, sub(1, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO, sub(1, BigDecimal.ONE));

        // LONG
        assertEquals(0L, sub(1L, 1L));
        assertEquals(0D, sub(1L, 1F));
        assertEquals(0D, sub(1L, 1D));
        assertEquals(BigInteger.ZERO, sub(1L, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO, sub(1L, BigDecimal.ONE));

        // FLOAT
        assertEquals(0F, sub(1F, 1F));
        assertEquals(0D, sub(1F, 1D));
        assertEquals(BigDecimal.ZERO.setScale(1, RoundingMode.HALF_UP), sub(1F, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO.setScale(1, RoundingMode.HALF_UP), sub(1F, BigDecimal.ONE));

        // DOUBLE
        assertEquals(0D, sub(1D, 1D));
        assertEquals(BigDecimal.ZERO.setScale(1, RoundingMode.HALF_UP), sub(1D, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO.setScale(1, RoundingMode.HALF_UP), sub(1D, BigDecimal.ONE));

        // BIG INTEGER
        assertEquals(BigInteger.ZERO, sub(BigInteger.ONE, BigInteger.ONE));
        assertEquals(BigDecimal.ZERO, sub(BigInteger.ONE, BigDecimal.ONE));

        // BIG DECIMAL
        assertEquals(BigDecimal.ZERO, sub(BigDecimal.ONE, BigDecimal.ONE));
    }

    @Test
    public void shouldMultiplyAndReturnCorrectType() {
        assertEquals((byte) 1, mul((byte) 1, (Byte) null));
        assertNull(mul((Byte) null, (byte) 1));

        // BYTE
        assertEquals((byte) 1, mul((byte) 1, (byte) 1));
        assertEquals((short) 1, mul((byte) 1, (short) 1));
        assertEquals(1, mul((byte) 1, 1));
        assertEquals(1L, mul((byte) 1, 1L));
        assertEquals(1F, mul((byte) 1, 1F));
        assertEquals(1D, mul((byte) 1, 1D));
        assertEquals(BigInteger.ONE, mul((byte) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, mul((byte) 1, BigDecimal.ONE));

        // SHORT
        assertEquals((short) 1, mul((short) 1, (short) 1));
        assertEquals(1, mul((short) 1, 1));
        assertEquals(1L, mul((short) 1, 1L));
        assertEquals(1F, mul((short) 1, 1F));
        assertEquals(1D, mul((short) 1, 1D));
        assertEquals(BigInteger.ONE, mul((short) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, mul((short) 1, BigDecimal.ONE));

        // INTEGER
        assertEquals(1, mul(1, 1));
        assertEquals(1L, mul(1, 1L));
        assertEquals(1F, mul(1, 1F));
        assertEquals(1D, mul(1, 1D));
        assertEquals(BigInteger.ONE, mul(1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, mul(1, BigDecimal.ONE));

        // LONG
        assertEquals(1L, mul(1L, 1L));
        assertEquals(1D, mul(1L, 1F));
        assertEquals(1D, mul(1L, 1D));
        assertEquals(BigInteger.ONE, mul(1L, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, mul(1L, BigDecimal.ONE));

        // FLOAT
        assertEquals(1F, mul(1F, 1F));
        assertEquals(1D, mul(1F, 1D));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), mul(1F, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), mul(1F, BigDecimal.ONE));

        // DOUBLE
        assertEquals(1D, mul(1D, 1D));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), mul(1D, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), mul(1D, BigDecimal.ONE));

        // BIG INTEGER
        assertEquals(BigInteger.ONE, mul(BigInteger.ONE, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, mul(BigInteger.ONE, BigDecimal.ONE));

        // BIG DECIMAL
        assertEquals(BigDecimal.ONE, mul(BigDecimal.ONE, BigDecimal.ONE));
    }

    @Test
    public void shouldDivideAndReturnCorrectType() {
        assertEquals((byte) 1, div((byte) 1, (Byte) null));
        assertNull(div((Byte) null, (byte) 1));

        // BYTE
        assertEquals((byte) 1, div((byte) 1, (byte) 1));
        assertEquals((short) 1, div((byte) 1, (short) 1));
        assertEquals(1, div((byte) 1, 1));
        assertEquals(1L, div((byte) 1, 1L));
        assertEquals(1F, div((byte) 1, 1F));
        assertEquals(1D, div((byte) 1, 1D));
        assertEquals(BigInteger.ONE, div((byte) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, div((byte) 1, BigDecimal.ONE));

        // SHORT
        assertEquals((short) 1, div((short) 1, (short) 1));
        assertEquals(1, div((short) 1, 1));
        assertEquals(1L, div((short) 1, 1L));
        assertEquals(1F, div((short) 1, 1F));
        assertEquals(1D, div((short) 1, 1D));
        assertEquals(BigInteger.ONE, div((short) 1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, div((short) 1, BigDecimal.ONE));

        // INTEGER
        assertEquals(1, div(1, 1));
        assertEquals(1L, div(1, 1L));
        assertEquals(1F, div(1, 1F));
        assertEquals(1D, div(1, 1D));
        assertEquals(BigInteger.ONE, div(1, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, div(1, BigDecimal.ONE));

        // LONG
        assertEquals(1L, div(1L, 1L));
        assertEquals(1D, div(1L, 1F));
        assertEquals(1D, div(1L, 1D));
        assertEquals(BigInteger.ONE, div(1L, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, div(1L, BigDecimal.ONE));

        // FLOAT
        assertEquals(1F, div(1F, 1F));
        assertEquals(1D, div(1F, 1D));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1F, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1F, BigDecimal.ONE));

        // DOUBLE
        assertEquals(1D, div(1D, 1D));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1D, BigInteger.ONE));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1D, BigDecimal.ONE));

        // BIG INTEGER
        assertEquals(BigInteger.ONE, div(BigInteger.ONE, BigInteger.ONE));
        assertEquals(BigDecimal.ONE, div(BigInteger.ONE, BigDecimal.ONE));

        // BIG DECIMAL
        assertEquals(BigDecimal.ONE, div(BigDecimal.ONE, BigDecimal.ONE));
    }

    @Test
    public void shouldDivideForceFloatingPointAndReturnCorrectType() {
        // BYTE
        assertEquals(1F, div((byte) 1, (byte) 1, true));
        assertEquals(1F, div((byte) 1, (short) 1, true));
        assertEquals(1F, div((byte) 1, 1, true));
        assertEquals(1D, div((byte) 1, 1L, true));
        assertEquals(1F, div((byte) 1, 1F, true));
        assertEquals(1D, div((byte) 1, 1D, true));
        assertEquals(BigDecimal.ONE, div((byte) 1, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE, div((byte) 1, BigDecimal.ONE, true));

        // SHORT
        assertEquals(1F, div((short) 1, (short) 1, true));
        assertEquals(1F, div((short) 1, 1, true));
        assertEquals(1D, div((short) 1, 1L, true));
        assertEquals(1F, div((short) 1, 1F, true));
        assertEquals(1D, div((short) 1, 1D, true));
        assertEquals(BigDecimal.ONE, div((short) 1, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE, div((short) 1, BigDecimal.ONE, true));

        // INTEGER
        assertEquals(1F, div(1, 1, true));
        assertEquals(1D, div(1, 1L, true));
        assertEquals(1F, div(1, 1F, true));
        assertEquals(1D, div(1, 1D, true));
        assertEquals(BigDecimal.ONE, div(1, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE, div(1, BigDecimal.ONE, true));

        // LONG
        assertEquals(1D, div(1L, 1L, true));
        assertEquals(1D, div(1L, 1F, true));
        assertEquals(1D, div(1L, 1D, true));
        assertEquals(BigDecimal.ONE, div(1L, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE, div(1L, BigDecimal.ONE, true));

        // FLOAT
        assertEquals(1F, div(1F, 1F, true));
        assertEquals(1D, div(1F, 1D, true));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1F, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1F, BigDecimal.ONE, true));

        // DOUBLE
        assertEquals(1D, div(1D, 1D, true));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1D, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE.setScale(1, RoundingMode.HALF_UP), div(1D, BigDecimal.ONE, true));

        // BIG INTEGER
        assertEquals(BigDecimal.ONE, div(BigInteger.ONE, BigInteger.ONE, true));
        assertEquals(BigDecimal.ONE, div(BigInteger.ONE, BigDecimal.ONE, true));

        // BIG DECIMAL
        assertEquals(BigDecimal.ONE, div(BigDecimal.ONE, BigDecimal.ONE, true));
    }

    @Test
    public void testMinMaxCompare() {

        final List<Number> zeros = Arrays.asList((byte) 0, (short) 0, 0, 0L, 0F, 0D, BigInteger.ZERO, BigDecimal.ZERO);
        final List<Number> ones = Arrays.asList((byte) 1, (short) 1, 1, 1L, 1F, 1D, BigInteger.ONE, BigDecimal.ONE);

        for (Number zero : zeros) {
            for (Number one : ones) {
                assertEquals(0, min(zero, one).intValue());
                assertEquals(0, min(one, zero).intValue());
                assertEquals(1, max(zero, one).intValue());
                assertEquals(1, max(one, zero).intValue());
                assertTrue(compare(zero, one) < 0);
                assertTrue(compare(one, zero) > 0);
                assertTrue(compare(zero, zero) == 0);
                assertTrue(compare(one, one) == 0);
            }
        }

        for (Number one : ones) {
            assertEquals(1, min(null, one).intValue());
            assertEquals(1, min(one, null).intValue());
            assertEquals(1, max(null, one).intValue());
            assertEquals(1, max(one, null).intValue());

            assertEquals(-1, compare(null, one).intValue());
            assertEquals(1, compare(one, null).intValue());
        }
    }
}
