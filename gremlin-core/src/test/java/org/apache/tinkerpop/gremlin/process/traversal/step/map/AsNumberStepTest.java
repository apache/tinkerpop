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

import org.apache.tinkerpop.gremlin.process.traversal.N;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.GType;
import org.apache.tinkerpop.gremlin.structure.GremlinDataType;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AsNumberStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.asNumber());
    }

    @Test
    public void testReturnTypes() {
        assertEquals(1, __.__(1).asNumber().next());
        assertEquals((byte) 1, __.__(1).asNumber(N.byte_).next());
        assertEquals(1, __.__(1.8).asNumber(N.int_).next());
        assertEquals(1, __.__(1L).asNumber(N.int_).next());
        assertEquals(1L, __.__(1L).asNumber().next());
        assertEquals(3.14f, __.__(3.14).asNumber(N.float_).next());
        assertEquals(3.14, __.__(3.14f).asNumber(N.double_).next());
        assertEquals(3.14, __.__(3.14f).asNumber(GType.DOUBLE).next());
        assertEquals(1, __.__("1").asNumber(N.int_).next());
        assertEquals(1, __.__("1").asNumber().next());
        assertEquals((byte) 1, __.__("1").asNumber(N.byte_).next());
        assertEquals((short) 1, __.__("1").asNumber(N.short_).next());
        assertEquals(1L, __.__("1").asNumber(N.long_).next());
        assertEquals(3.14, __.__("3.14").asNumber(N.double_).next()); //float to double
        // NumberUtils allows additional string processing
        assertEquals(123.0f, __.__("123.").asNumber().next());
        assertEquals(291, __.__("0x123").asNumber().next());
        assertEquals(83, __.__("0123").asNumber().next());
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

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenParsedNumberOverflows() {
        __.__("128").asNumber(N.byte_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenCastNumberOverflows() {
        __.__(128).asNumber(N.byte_).next();
    }

    @Test
    public void testStringToByte() {
        assertEquals((byte) 0, __.__("0").asNumber(N.byte_).next());
        assertEquals((byte) 127, __.__("127").asNumber(N.byte_).next());
        assertEquals((byte) -128, __.__("-128").asNumber(N.byte_).next());
        assertEquals((byte) 42, __.__("42").asNumber(N.byte_).next());
        assertEquals((byte) -42, __.__("-42").asNumber(N.byte_).next());
        assertEquals((byte) 1, __.__("1").asNumber(N.byte_).next());
    }

    @Test
    public void testStringToShort() {
        assertEquals((short) 0, __.__("0").asNumber(N.short_).next());
        assertEquals((short) 32767, __.__("32767").asNumber(N.short_).next());
        assertEquals((short) -32768, __.__("-32768").asNumber(N.short_).next());
        assertEquals((short) 1000, __.__("1000").asNumber(N.short_).next());
        assertEquals((short) -1000, __.__("-1000").asNumber(N.short_).next());
        assertEquals((short) 255, __.__("255").asNumber(N.short_).next());
    }

    @Test
    public void testStringToInt() {
        assertEquals(0, __.__("0").asNumber(N.int_).next());
        assertEquals(2147483647, __.__("2147483647").asNumber(N.int_).next());
        assertEquals(-2147483648, __.__("-2147483648").asNumber(N.int_).next());
        assertEquals(123456, __.__("123456").asNumber(N.int_).next());
        assertEquals(-123456, __.__("-123456").asNumber(N.int_).next());
        assertEquals(65536, __.__("65536").asNumber(N.int_).next());
    }

    @Test
    public void testStringToLong() {
        assertEquals(0L, __.__("0").asNumber(N.long_).next());
        assertEquals(9223372036854775807L, __.__("9223372036854775807").asNumber(N.long_).next());
        assertEquals(-9223372036854775808L, __.__("-9223372036854775808").asNumber(N.long_).next());
        assertEquals(123456789L, __.__("123456789").asNumber(N.long_).next());
        assertEquals(-123456789L, __.__("-123456789").asNumber(N.long_).next());
        assertEquals(4294967296L, __.__("4294967296").asNumber(N.long_).next());
    }

    @Test
    public void testStringToFloat() {
        assertEquals(0.0f, __.__("0.0").asNumber(N.float_).next());
        assertEquals(3.14f, __.__("3.14").asNumber(N.float_).next());
        assertEquals(-3.14f, __.__("-3.14").asNumber(N.float_).next());
        assertEquals(1.23e10f, __.__("1.23e10").asNumber(N.float_).next());
        assertEquals(-1.23e-10f, __.__("-1.23e-10").asNumber(N.float_).next());
        assertEquals(Float.MAX_VALUE, __.__("3.4028235E38").asNumber(N.float_).next());
        assertEquals(Float.MIN_VALUE, __.__("1.4E-45").asNumber(N.float_).next());
    }

    @Test
    public void testStringToDouble() {
        assertEquals(0.0, __.__("0.0").asNumber(N.double_).next());
        assertEquals(3.141592653589793, __.__("3.141592653589793").asNumber(N.double_).next());
        assertEquals(-3.141592653589793, __.__("-3.141592653589793").asNumber(N.double_).next());
        assertEquals(1.23e100, __.__("1.23e100").asNumber(N.double_).next());
        assertEquals(-1.23e-100, __.__("-1.23e-100").asNumber(N.double_).next());
        assertEquals(Double.MAX_VALUE, __.__("1.7976931348623157E308").asNumber(N.double_).next());
        assertEquals(Double.MIN_VALUE, __.__("4.9E-324").asNumber(N.double_).next());
    }

    @Test
    public void testStringToBigInteger() {
        assertEquals(new BigInteger("0"), __.__("0").asNumber(N.bigInt).next());
        assertEquals(new BigInteger("123456789012345678901234567890"),
                __.__("123456789012345678901234567890").asNumber(N.bigInt).next());
        assertEquals(new BigInteger("-123456789012345678901234567890"),
                __.__("-123456789012345678901234567890").asNumber(N.bigInt).next());
        assertEquals(new BigInteger("999999999999999999999999999999999999999999999999999"),
                __.__("999999999999999999999999999999999999999999999999999").asNumber(N.bigInt).next());
        assertEquals(new BigInteger("1"), __.__("1").asNumber(N.bigInt).next());
        assertEquals(new BigInteger("1000000000000000000000"), __.__(new Double("1000000000000000000000")).asNumber(N.bigInt).next());
    }

    @Test
    public void testStringToBigDecimal() {
        assertEquals(new BigDecimal("0.0"), __.__("0.0").asNumber(N.bigDecimal).next());
        assertEquals(new BigDecimal("123456789012345678901234567890.123456789"),
                __.__("123456789012345678901234567890.123456789").asNumber(N.bigDecimal).next());
        assertEquals(new BigDecimal("-123456789012345678901234567890.123456789"),
                __.__("-123456789012345678901234567890.123456789").asNumber(N.bigDecimal).next());
        // Note directly constructing BigDecimal returns 1E-39, but parsing then converting from double results in 1.0E-39
        assertEquals(BigDecimal.valueOf(Double.parseDouble("0.000000000000000000000000000000000000001")),
                __.__("0.000000000000000000000000000000000000001").asNumber(N.bigDecimal).next());
        assertEquals(new BigDecimal("1.0"), __.__("1.0").asNumber(N.bigDecimal).next());
    }

// ===== EDGE CASE TESTS =====

    @Test
    public void testCastInfinityAndNaN() {
        assertEquals(Double.POSITIVE_INFINITY, __.__(Float.POSITIVE_INFINITY).asNumber(N.double_).next());
        assertEquals(Double.NEGATIVE_INFINITY, __.__(Float.NEGATIVE_INFINITY).asNumber(N.double_).next());
        assertEquals(Double.NaN, __.__(Float.NaN).asNumber(N.double_).next());
        assertEquals(Float.POSITIVE_INFINITY, __.__(Double.POSITIVE_INFINITY).asNumber(N.float_).next());
        assertEquals(Float.NEGATIVE_INFINITY, __.__(Double.NEGATIVE_INFINITY).asNumber(N.float_).next());
        assertEquals(Float.NaN, __.__(Double.NaN).asNumber(N.float_).next());
    }

    @Test
    public void testStringWithWhitespace() {
        assertEquals(42, __.__(" 42 ").asNumber(N.int_).next());
        assertEquals(42, __.__("\t42\n").asNumber(N.int_).next());
        assertEquals(3.14, __.__(" 3.14 ").asNumber(N.double_).next());
        assertEquals((byte) 127, __.__(" 127 ").asNumber(N.byte_).next());
    }

    @Test
    public void testStringWithPlusSign() {
        assertEquals(42, __.__("+42").asNumber(N.int_).next());
        assertEquals(3.14, __.__("+3.14").asNumber(N.double_).next());
        assertEquals((byte) 42, __.__("+42").asNumber(N.byte_).next());
        assertEquals(new BigInteger("42"), __.__("+42").asNumber(N.bigInt).next());
    }

    @Test
    public void testZeroValues() {
        assertEquals((byte) 0, __.__("0").asNumber(N.byte_).next());
        assertEquals((short) 0, __.__("0").asNumber(N.short_).next());
        assertEquals(0, __.__("0").asNumber(N.int_).next());
        assertEquals(0L, __.__("0").asNumber(N.long_).next());
        assertEquals(0.0f, __.__("0.0").asNumber(N.float_).next());
        assertEquals(0.0, __.__("0.0").asNumber(N.double_).next());
        assertEquals(new BigInteger("0"), __.__("0").asNumber(N.bigInt).next());
        assertEquals(new BigDecimal("0.0"), __.__("0.0").asNumber(N.bigDecimal).next());
    }

    @Test
    public void testScientificNotation() {
        assertEquals(1.23e10f, __.__("1.23e10").asNumber(N.float_).next());
        assertEquals(1.23e10, __.__("1.23e10").asNumber(N.double_).next());
        assertEquals(1.23e-10f, __.__("1.23e-10").asNumber(N.float_).next());
        assertEquals(1.23e-10, __.__("1.23e-10").asNumber(N.double_).next());
        assertEquals(new BigDecimal("1.23E10"), __.__("1.23e10").asNumber(N.bigDecimal).next());
    }

// ===== OVERFLOW EXCEPTION TESTS =====

    @Test(expected = ArithmeticException.class)
    public void shouldThrowCastExceptionConvertingInfinityToWholeNumber() {
        __.__(Double.POSITIVE_INFINITY).asNumber(N.long_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowCastExceptionConvertingNaNToWholeNumber() {
        __.__(Double.NaN).asNumber(N.int_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringByteOverflowsPositive() {
        __.__("128").asNumber(N.byte_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringByteOverflowsNegative() {
        __.__("-129").asNumber(N.byte_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringShortOverflowsPositive() {
        __.__("32768").asNumber(N.short_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringShortOverflowsNegative() {
        __.__("-32769").asNumber(N.short_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalToShortOverflows() {
        __.__(new BigDecimal(Long.MAX_VALUE+"1")).asNumber(N.short_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenFloatToLongOverflows() {
        __.__(new Float(Long.MAX_VALUE+"1")).asNumber(N.long_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenDoubleToLongOverflows() {
        __.__(new Double(Long.MAX_VALUE+"1")).asNumber(N.long_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringIntOverflowsPositive() {
        __.__("2147483648").asNumber(N.int_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringIntOverflowsNegative() {
        __.__("-2147483649").asNumber(N.int_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringLongOverflowsPositive() {
        __.__("9223372036854775809").asNumber(N.long_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenStringLongOverflowsNegative() {
        __.__("-9223372036854775809").asNumber(N.long_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenDoubleStringToFloatOverflows() {
        __.__("3.5E38").asNumber(N.float_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalStringToDoubleOverflows() {
        __.__("1.8E308").asNumber(N.double_).next();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldThrowOverflowExceptionWhenBigDecimalStringToFloatOverflows() {
        __.__("1.8E308").asNumber(N.float_).next();
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
        assertEquals(Byte.MAX_VALUE, __.__("127").asNumber(N.byte_).next());
        assertEquals(Byte.MIN_VALUE, __.__("-128").asNumber(N.byte_).next());
        assertEquals(Short.MAX_VALUE, __.__("32767").asNumber(N.short_).next());
        assertEquals(Short.MIN_VALUE, __.__("-32768").asNumber(N.short_).next());
        assertEquals(Integer.MAX_VALUE, __.__("2147483647").asNumber(N.int_).next());
        assertEquals(Integer.MIN_VALUE, __.__("-2147483648").asNumber(N.int_).next());
        assertEquals(Long.MAX_VALUE, __.__("9223372036854775807").asNumber(N.long_).next());
        assertEquals(Long.MIN_VALUE, __.__("-9223372036854775808").asNumber(N.long_).next());
    }

    @Test
    public void testHighPrecisionDecimals() {
        assertEquals(new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067"),
                __.__("3.141592653589793238462643383279502884197169399375105820974944592307816406286208998628034825342117067").asNumber(N.bigDecimal).next());
    }

    @Test
    public void testVeryLargeNumbers() {
        String largeNumber = "12345678901234567890123456789012345678901234567890123456789012345678901234567890";
        assertEquals(new BigInteger(largeNumber), __.__(largeNumber).asNumber(N.bigInt).next());
    }

}
