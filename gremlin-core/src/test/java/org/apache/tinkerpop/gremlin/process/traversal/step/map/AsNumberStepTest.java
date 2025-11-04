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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Date;
import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AsNumberStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asNumber());
    }

    @Test
    public void testReturnTypes() {
        assertEquals(1, __.__(1).asNumber().next());
        assertEquals((byte) 1, __.__(1).asNumber(GType.BYTE).next());
        assertEquals(1, __.__(1.8).asNumber(GType.INT).next());
        assertEquals(1, __.__(1L).asNumber(GType.INT).next());
        assertEquals(1L, __.__(1L).asNumber().next());
        assertEquals(3.14f, __.__(3.14).asNumber(GType.FLOAT).next());
        assertEquals(3.14, __.__(3.14f).asNumber(GType.DOUBLE).next());
        assertEquals(3.14, __.__(3.14f).asNumber(GType.DOUBLE).next());
        assertEquals(3, __.__(new BigDecimal("3.14")).asNumber(GType.INT).next());
        assertEquals(3L, __.__(new BigDecimal("3.14")).asNumber(GType.LONG).next());
        assertEquals(-3, __.__(new BigDecimal("-3.14")).asNumber(GType.INT).next());
        assertEquals(-3L, __.__(new BigDecimal("-3.14")).asNumber(GType.LONG).next());
        assertEquals(1, __.__("1").asNumber(GType.INT).next());
        assertEquals(1, __.__("1").asNumber().next());
        assertEquals((byte) 1, __.__("1").asNumber(GType.BYTE).next());
        assertEquals((short) 1, __.__("1").asNumber(GType.SHORT).next());
        assertEquals(1L, __.__("1").asNumber(GType.LONG).next());
        assertEquals(3.14, __.__("3.14").asNumber(GType.DOUBLE).next()); //float to double
        // round trip date to number
        final OffsetDateTime date = OffsetDateTime.of(LocalDateTime.of(2025, 11, 3, 7, 20, 19, 0), UTC);
        final long dateEpochMillis = date.toInstant().toEpochMilli();
        assertEquals(date, __.__(date).asNumber().asDate().next());
        assertEquals(dateEpochMillis, __.__(dateEpochMillis).asDate().asNumber().next());
        assertEquals(dateEpochMillis, __.__(new Date(dateEpochMillis)).asNumber().next());
        assertEquals(date, __.__(new Date(dateEpochMillis)).asNumber().asDate().next());
        // NumberUtils allows additional string processing
        assertEquals(123.0f, __.__("123.").asNumber().next());
        assertEquals(291, __.__("0x123").asNumber().next());
        assertEquals(83, __.__("0123").asNumber().next());
        assertNull(__.__((Object) null).asNumber().next());
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenInvalidStringInput() {
        __.__("This String is not a number").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowParseExceptionWithInvalidNumberStringInput() {
        __.__("128abc").asNumber().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenArrayInput() {
        __.__(Arrays.asList(1, 2)).asNumber().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenUUIDInput() {
        __.__(UUID.randomUUID()).asNumber().next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWithWrongGTypeInput() {
        __.__("128").asNumber(GType.VERTEX).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenParsedNumberOverflows() {
        __.__("128").asNumber(GType.BYTE).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenCastNumberOverflows() {
        __.__(128).asNumber(GType.BYTE).next();
    }

    @Test
    public void testStringToByte() {
        assertEquals((byte) 0, __.__("0").asNumber(GType.BYTE).next());
        assertEquals((byte) 127, __.__("127").asNumber(GType.BYTE).next());
        assertEquals((byte) -128, __.__("-128").asNumber(GType.BYTE).next());
        assertEquals((byte) 42, __.__("42").asNumber(GType.BYTE).next());
        assertEquals((byte) -42, __.__("-42").asNumber(GType.BYTE).next());
        assertEquals((byte) 1, __.__("1").asNumber(GType.BYTE).next());
    }

    @Test
    public void testStringToShort() {
        assertEquals((short) 0, __.__("0").asNumber(GType.SHORT).next());
        assertEquals((short) 32767, __.__("32767").asNumber(GType.SHORT).next());
        assertEquals((short) -32768, __.__("-32768").asNumber(GType.SHORT).next());
        assertEquals((short) 1000, __.__("1000").asNumber(GType.SHORT).next());
        assertEquals((short) -1000, __.__("-1000").asNumber(GType.SHORT).next());
        assertEquals((short) 255, __.__("255").asNumber(GType.SHORT).next());
    }

    @Test
    public void testStringToInt() {
        assertEquals(0, __.__("0").asNumber(GType.INT).next());
        assertEquals(2147483647, __.__("2147483647").asNumber(GType.INT).next());
        assertEquals(-2147483648, __.__("-2147483648").asNumber(GType.INT).next());
        assertEquals(123456, __.__("123456").asNumber(GType.INT).next());
        assertEquals(-123456, __.__("-123456").asNumber(GType.INT).next());
        assertEquals(65536, __.__("65536").asNumber(GType.INT).next());
    }

    @Test
    public void testStringToLong() {
        assertEquals(0L, __.__("0").asNumber(GType.LONG).next());
        assertEquals(9223372036854775807L, __.__("9223372036854775807").asNumber(GType.LONG).next());
        assertEquals(-9223372036854775808L, __.__("-9223372036854775808").asNumber(GType.LONG).next());
        assertEquals(123456789L, __.__("123456789").asNumber(GType.LONG).next());
        assertEquals(-123456789L, __.__("-123456789").asNumber(GType.LONG).next());
        assertEquals(4294967296L, __.__("4294967296").asNumber(GType.LONG).next());
    }

    @Test
    public void testStringToFloat() {
        assertEquals(0.0f, __.__("0.0").asNumber(GType.FLOAT).next());
        assertEquals(3.14f, __.__("3.14").asNumber(GType.FLOAT).next());
        assertEquals(-3.14f, __.__("-3.14").asNumber(GType.FLOAT).next());
        assertEquals(1.23e10f, __.__("1.23e10").asNumber(GType.FLOAT).next());
        assertEquals(-1.23e-10f, __.__("-1.23e-10").asNumber(GType.FLOAT).next());
        assertEquals(Float.MAX_VALUE, __.__("3.4028235E38").asNumber(GType.FLOAT).next());
        assertEquals(Float.MIN_VALUE, __.__("1.4E-45").asNumber(GType.FLOAT).next());
    }

    @Test
    public void testStringToDouble() {
        assertEquals(0.0, __.__("0.0").asNumber(GType.DOUBLE).next());
        assertEquals(3.141592653589793, __.__("3.141592653589793").asNumber(GType.DOUBLE).next());
        assertEquals(-3.141592653589793, __.__("-3.141592653589793").asNumber(GType.DOUBLE).next());
        assertEquals(1.23e100, __.__("1.23e100").asNumber(GType.DOUBLE).next());
        assertEquals(-1.23e-100, __.__("-1.23e-100").asNumber(GType.DOUBLE).next());
        assertEquals(Double.MAX_VALUE, __.__("1.7976931348623157E308").asNumber(GType.DOUBLE).next());
        assertEquals(Double.MIN_VALUE, __.__("4.9E-324").asNumber(GType.DOUBLE).next());
    }

    @Test
    public void testStringToBigInteger() {
        assertEquals(new BigInteger("0"), __.__("0").asNumber(GType.BIGINT).next());
        assertEquals(new BigInteger("123456789012345678901234567890"),
                __.__("123456789012345678901234567890").asNumber(GType.BIGINT).next());
        assertEquals(new BigInteger("-123456789012345678901234567890"),
                __.__("-123456789012345678901234567890").asNumber(GType.BIGINT).next());
        assertEquals(new BigInteger("999999999999999999999999999999999999999999999999999"),
                __.__("999999999999999999999999999999999999999999999999999").asNumber(GType.BIGINT).next());
        assertEquals(new BigInteger("1"), __.__("1").asNumber(GType.BIGINT).next());
        assertEquals(new BigInteger("1000000000000000000000"), __.__(new Double("1000000000000000000000")).asNumber(GType.BIGINT).next());
    }

    @Test
    public void testStringToBigDecimal() {
        assertEquals(new BigDecimal("0.0"), __.__("0.0").asNumber(GType.BIGDECIMAL).next());
        assertEquals(new BigDecimal("123456789012345678901234567890.123456789"),
                __.__("123456789012345678901234567890.123456789").asNumber(GType.BIGDECIMAL).next());
        assertEquals(new BigDecimal("-123456789012345678901234567890.123456789"),
                __.__("-123456789012345678901234567890.123456789").asNumber(GType.BIGDECIMAL).next());
        // Note directly constructing BigDecimal returns 1E-39, but parsing then converting from double results in 1.0E-39
        assertEquals(BigDecimal.valueOf(Double.parseDouble("0.000000000000000000000000000000000000001")),
                __.__("0.000000000000000000000000000000000000001").asNumber(GType.BIGDECIMAL).next());
        assertEquals(new BigDecimal("1.0"), __.__("1.0").asNumber(GType.BIGDECIMAL).next());
    }

// ===== EDGE CASE TESTS =====

    @Test
    public void testCastInfinityAndNaN() {
        assertEquals(Double.POSITIVE_INFINITY, __.__(Float.POSITIVE_INFINITY).asNumber(GType.DOUBLE).next());
        assertEquals(Double.NEGATIVE_INFINITY, __.__(Float.NEGATIVE_INFINITY).asNumber(GType.DOUBLE).next());
        assertEquals(Double.NaN, __.__(Float.NaN).asNumber(GType.DOUBLE).next());
        assertEquals(Float.POSITIVE_INFINITY, __.__(Double.POSITIVE_INFINITY).asNumber(GType.FLOAT).next());
        assertEquals(Float.NEGATIVE_INFINITY, __.__(Double.NEGATIVE_INFINITY).asNumber(GType.FLOAT).next());
        assertEquals(Float.NaN, __.__(Double.NaN).asNumber(GType.FLOAT).next());
    }

    @Test
    public void testStringWithWhitespace() {
        assertEquals(42, __.__(" 42 ").asNumber(GType.INT).next());
        assertEquals(42, __.__("\t42\n").asNumber(GType.INT).next());
        assertEquals(3.14, __.__(" 3.14 ").asNumber(GType.DOUBLE).next());
        assertEquals((byte) 127, __.__(" 127 ").asNumber(GType.BYTE).next());
    }

    @Test
    public void testStringWithPlusSign() {
        assertEquals(42, __.__("+42").asNumber(GType.INT).next());
        assertEquals(3.14, __.__("+3.14").asNumber(GType.DOUBLE).next());
        assertEquals((byte) 42, __.__("+42").asNumber(GType.BYTE).next());
        assertEquals(new BigInteger("42"), __.__("+42").asNumber(GType.BIGINT).next());
    }

    @Test
    public void testZeroValues() {
        assertEquals((byte) 0, __.__("0").asNumber(GType.BYTE).next());
        assertEquals((short) 0, __.__("0").asNumber(GType.SHORT).next());
        assertEquals(0, __.__("0").asNumber(GType.INT).next());
        assertEquals(0L, __.__("0").asNumber(GType.LONG).next());
        assertEquals(0.0f, __.__("0.0").asNumber(GType.FLOAT).next());
        assertEquals(0.0, __.__("0.0").asNumber(GType.DOUBLE).next());
        assertEquals(new BigInteger("0"), __.__("0").asNumber(GType.BIGINT).next());
        assertEquals(new BigDecimal("0.0"), __.__("0.0").asNumber(GType.BIGDECIMAL).next());
    }

    @Test
    public void testScientificNotation() {
        assertEquals(1.23e10f, __.__("1.23e10").asNumber(GType.FLOAT).next());
        assertEquals(1.23e10, __.__("1.23e10").asNumber(GType.DOUBLE).next());
        assertEquals(1.23e-10f, __.__("1.23e-10").asNumber(GType.FLOAT).next());
        assertEquals(1.23e-10, __.__("1.23e-10").asNumber(GType.DOUBLE).next());
        assertEquals(new BigDecimal("1.23E10"), __.__("1.23e10").asNumber(GType.BIGDECIMAL).next());
    }

// ===== OVERFLOW EXCEPTION TESTS =====

    @Test(expected = ArithmeticException.class)
    public void shouldThrowCastExceptionConvertingInfinityToWholeNumber() {
        __.__(Double.POSITIVE_INFINITY).asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowCastExceptionConvertingNaNToWholeNumber() {
        __.__(Double.NaN).asNumber(GType.INT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringByteOverflowsPositive() {
        __.__("128").asNumber(GType.BYTE).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringByteOverflowsNegative() {
        __.__("-129").asNumber(GType.BYTE).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringShortOverflowsPositive() {
        __.__("32768").asNumber(GType.SHORT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringShortOverflowsNegative() {
        __.__("-32769").asNumber(GType.SHORT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalToShortOverflows() {
        __.__(new BigDecimal(Long.MAX_VALUE+"1")).asNumber(GType.SHORT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalToLongOverflows() {
        __.__(new BigDecimal(Long.MAX_VALUE+"1")).asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenFloatToLongOverflows() {
        __.__(new Float(Long.MAX_VALUE+"1")).asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenDoubleToLongOverflows() {
        __.__(new Double(Long.MAX_VALUE+"1")).asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringIntOverflowsPositive() {
        __.__("2147483648").asNumber(GType.INT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringIntOverflowsNegative() {
        __.__("-2147483649").asNumber(GType.INT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringLongOverflowsPositive() {
        __.__("9223372036854775809").asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringLongOverflowsNegative() {
        __.__("-9223372036854775809").asNumber(GType.LONG).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenDoubleStringToFloatOverflows() {
        __.__("3.5E38").asNumber(GType.FLOAT).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalStringToDoubleOverflows() {
        __.__("1.8E308").asNumber(GType.DOUBLE).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalStringToFloatOverflows() {
        __.__("1.8E308").asNumber(GType.FLOAT).next();
    }

// ===== INVALID FORMAT EXCEPTION TESTS =====

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenEmptyString() {
        __.__("").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenWhitespaceOnlyString() {
        __.__("   ").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenAlphabeticString() {
        __.__("abc").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenMixedAlphanumericString() {
        __.__("123abc456").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenSpecialCharactersString() {
        __.__("12@34").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenMultipleDecimalPoints() {
        __.__("12.34.56").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenMultipleSigns() {
        __.__("++123").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenSignInMiddle() {
        __.__("12+34").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenInvalidScientificNotation() {
        __.__("12e").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenInvalidScientificNotationWithSign() {
        __.__("12e+").asNumber().next();
    }

    @Test(expected = NumberFormatException.class)
    public void shouldThrowExceptionWhenInvalidDecimalFormat() {
        __.__(".").asNumber().next();
    }

// ===== PRECISION AND BOUNDARY TESTS =====

    @Test
    public void testBoundaryValues() {
        assertEquals(Byte.MAX_VALUE, __.__("127").asNumber(GType.BYTE).next());
        assertEquals(Byte.MIN_VALUE, __.__("-128").asNumber(GType.BYTE).next());
        assertEquals(Short.MAX_VALUE, __.__("32767").asNumber(GType.SHORT).next());
        assertEquals(Short.MIN_VALUE, __.__("-32768").asNumber(GType.SHORT).next());
        assertEquals(Integer.MAX_VALUE, __.__("2147483647").asNumber(GType.INT).next());
        assertEquals(Integer.MIN_VALUE, __.__("-2147483648").asNumber(GType.INT).next());
        assertEquals(Long.MAX_VALUE, __.__("9223372036854775807").asNumber(GType.LONG).next());
        assertEquals(Long.MIN_VALUE, __.__("-9223372036854775808").asNumber(GType.LONG).next());
    }

    @Test
    public void testHighPrecisionDecimals() {
        assertEquals(new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067"),
                __.__("3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067").asNumber(GType.BIGDECIMAL).next());
    }

    @Test
    public void testVeryLargeNumbers() {
        String largeNumber = "12345678901234567890123456789012345678901234567890123456789012345678901234567890";
        assertEquals(new BigInteger(largeNumber), __.__(largeNumber).asNumber(GType.BIGINT).next());
    }

}
