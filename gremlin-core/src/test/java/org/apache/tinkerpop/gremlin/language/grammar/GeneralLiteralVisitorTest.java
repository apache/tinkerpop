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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitiveProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.time.ZoneOffset.UTC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Generic Literal visitor test
 */
@RunWith(Enclosed.class)
public class GeneralLiteralVisitorTest {

    static Object parseGenericLiteralRange(final String query) {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(query));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.GenericRangeLiteralContext ctx = parser.genericRangeLiteral();
        return new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericRangeLiteral(ctx);
    }

    @RunWith(Parameterized.class)
    public static class ValidIntegerRangeTest {

        @Parameterized.Parameter(value = 0)
        public int start;

        @Parameterized.Parameter(value = 1)
        public int end;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {1, 1000000},
                    {1, 1},
                    {1000000, 1},
            });
        }

        @Test
        public void shouldParse() {
            assertThat(Math.abs(end - start), lessThan(GenericLiteralVisitor.TOTAL_INTEGER_RANGE_RESULT_COUNT_LIMIT));

            final Object result = parseGenericLiteralRange(String.format("%d..%d", start, end));
            assertThat(result, instanceOf(List.class));
            int expectedValue = start;
            final List<Object> results = (List<Object>) result;

            // iterate the result and check they are as expected
            for (Object actualValue : results) {
                assertThat(actualValue, instanceOf(Integer.class));
                assertEquals(expectedValue, actualValue);
                if (start <= end) {
                    expectedValue++;
                } else {
                    expectedValue--;
                }
            }
            assertEquals(Math.abs(end - start) + 1, results.size());
        }
    }

    @RunWith(Parameterized.class)
    public static class InvalidIntegerRangeTest {
        @Parameterized.Parameter(value = 0)
        public int start;

        @Parameterized.Parameter(value = 1)
        public int end;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {0, 10000000},
                    {10000000, 1},
            });
        }

        /**
         * test integer range exceed limit
         */
        @Test(expected = IllegalArgumentException.class)
        public void shouldNotParse() {
            assertThat(Math.abs(end - start), greaterThanOrEqualTo(GenericLiteralVisitor.TOTAL_INTEGER_RANGE_RESULT_COUNT_LIMIT));
            parseGenericLiteralRange(String.format("%d..%d", start, end));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidStringRangeTest {
        @Parameterized.Parameter(value = 0)
        public String start;

        @Parameterized.Parameter(value = 1)
        public String end;

        @Parameterized.Parameter(value = 2)
        public String expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"'abc1'", "'abc3'", "abc1_abc2_abc3"},
                    {"\"abc1\"", "'abc3'", "abc1_abc2_abc3"},
                    {"\"abc1\"", "\"abc3\"", "abc1_abc2_abc3"},
                    {"'abc1'", "\"abc3\"", "abc1_abc2_abc3"},
                    {"'a11a'", "'a11d'", "a11a_a11b_a11c_a11d"},
                    {"'a11N'", "'a11L'", "a11N_a11M_a11L"},
                    {"'a113'", "'a111'", "a113_a112_a111"},
                    {"'a111'", "'a111'", "a111"},
                    {"'a'", "'c'", "a_b_c"},
                    {"'6'", "'3'", "6_5_4_3"},
                    {"'1'", "'1'", "1"},
                    {"'N'", "'N'", "N"},
                    {"''", "''", "_"},
            });
        }

        @Test
        public void shouldParse() {
            final Object result = parseGenericLiteralRange(String.format("%s..%s", start, end));
            assertThat(result, instanceOf(List.class));

            final List<Object> results = (List<Object>) result;
            if (expected.equals("_")) {
                // handle special case for empty range
                assertEquals(0, results.size());
                return;
            }

            final String[] expectedResults = expected.split("_");
            assertEquals(expectedResults.length, results.size());

            // iterate the result and check they are as expected
            for (int i = 0; i < expectedResults.length; i++) {
                assertEquals(expectedResults[i], results.get(i));
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class InvalidStringRangeTest {
        @Parameterized.Parameter(value = 0)
        public String start;

        @Parameterized.Parameter(value = 1)
        public String end;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"'abc1'", "'abc23'"}, // start and end string length is different
                    {"a11", "'a22'"},      // start and end string does not share the same prefix except last character
                    {"''", "'1'"},         // start and end string length is different
                    {"'1'", "''"},         // start and end string length is different
            });
        }

        @Test(expected = IllegalArgumentException.class)
        public void shouldNotParse() {
            parseGenericLiteralRange(String.format("%s..%s", start, end));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidStringLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public String expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"'a'", "a"},
                    {"'A1'", "A1"},
                    {"'1'", "1"},
                    {"''", "Empty"},
                    {"'10_000'", "10_000"},
                    {"\"a\"", "a"},
                    {"\"A1\"", "A1"},
                    {"\"1\"", "1"},
                    {"\"\"", "Empty"},
                    {"\"10_000\"", "10_000"},
                    // escaped characters according to http://groovy-lang.org/syntax.html#_escaping_special_characters
                    // {} are there just for readability
                    {"'{\\t} {\\b} {\\n} {\\r} {\\f} {\\'} {\\\"} {\\\\}'", "{\t} {\b} {\n} {\r} {\f} {'} {\"} {\\}"},
                    {"\"{\\t} {\\b} {\\n} {\\r} {\\f} {\\'} {\\\"} {\\\\}\"", "{\t} {\b} {\n} {\r} {\f} {'} {\"} {\\}"},
                    // unicode escape
                    {"'\\u2300'", "\u2300"},
                    {"'abc\\u2300def'", "abc\u2300def"},
                    {"'\u2300'", "\u2300"},
                    {"'abc\u2300def'", "abc\u2300def"},
                    // explicit 's' suffix for string literals
                    {"\"hello\"s", "hello"},
                    {"'hello's", "hello"},
                    {"\"\"s", "Empty"},
                    {"\"a\"s", "a"},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.StringLiteralContext ctx = parser.stringLiteral();
            if (expected.equals("Empty")) {
                // handle special case for Empty string
                assertEquals("", new GenericLiteralVisitor(new GremlinAntlrToJava()).visitStringLiteral(ctx));
            } else {
                assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitStringLiteral(ctx));
            }
        }
    }

    public static class ValidStringNullableLiteralTest {
        @Test
        public void shouldParseStringSuffixAsNullableLiteral() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("\"person\"s"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.StringNullableLiteralContext ctx = parser.stringNullableLiteral();
            assertEquals("person", new GenericLiteralVisitor(new GremlinAntlrToJava()).visitStringNullableLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class AllPrintableAsciiCharactersTest {
        @Parameterized.Parameter(value = 0)
        public String quoteCharacter;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"\""},
                    {"'"},
            });
        }

        @Test
        public void shouldParse() {
            final StringBuilder inputBuilder = new StringBuilder();
            final StringBuilder expectedOutputBuilder = new StringBuilder();

            // build a string which contains all the ASCII printable characters
            // ASCII printable character start from 32 to 126
            for (Character c = 32; c < 127; c++) {
                if ((quoteCharacter.equals(String.valueOf(c))) || (c == '\\')) {
                    // escape this character in the input
                    inputBuilder.append("\\");
                }
                inputBuilder.append(c);
                expectedOutputBuilder.append(c);
            }
            final String inputChars = inputBuilder.toString();
            final String expectedOutputChars = expectedOutputBuilder.toString();

            final String stringLiteral = quoteCharacter + inputChars + quoteCharacter;
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(stringLiteral));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.StringLiteralContext ctx = parser.stringLiteral();
            assertEquals(expectedOutputChars, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitStringLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidIntegerLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public Object expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    // decimal format
                    {"1", 1},
                    {"-11", -11},
                    {"0", 0},
                    {"1B", (byte) 1},
                    {"-1b", (byte) -1},
                    {"1S", (short) 1},
                    {"-1s", (short) -1},
                    {"1I", 1},
                    {"-1i", -1},
                    {"1L", 1L},
                    {"-1l", -1L},
                    {"1_2_3", 123},
                    {"-1_2_3L", -123L},
                    {"1N", new BigInteger("1")},
                    {"-1n", new BigInteger("-1")},
                    {"9223372036854775807", 9223372036854775807L},
                    {"-9223372036854775808", -9223372036854775808L},
                    {"9223372036854775807L", 9223372036854775807L},
                    {"-9223372036854775808l", -9223372036854775808L},
                    {"9999999999999999999999999999999999999999999999999N", new BigInteger("9999999999999999999999999999999999999999999999999")},
                    {"9999999999999999999999999999999999999999999999999n", new BigInteger("9999999999999999999999999999999999999999999999999")},
                    // hex format
                    {"0xA", 10},
                    {"-0xA", -10},
                    {"0xaL", 10L},
                    {"-0xal", -10L},
                    {"-0xA_0L", -160L},
                    {"0x10", 16},
                    {"-0x10", -16},
                    {"0x10", 16},
                    {"-0x10l", -16L},
                    {"-0x1_0L", -16L},
                    // oct format
                    {"01", 1},
                    {"-01", -1},
                    {"01L", 1L},
                    {"-01l", -1L},
                    {"010", 8},
                    {"-010", -8},
                    {"010L", 8L},
                    {"-010l", -8L},
                    {"-01_0L", -8L},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.IntegerLiteralContext ctx = parser.integerLiteral();

            final Object actualValue = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitIntegerLiteral(ctx);
            assertEquals(expected, actualValue);
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidBigIntegerLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public String expected;

        @Parameterized.Parameter(value = 2)
        public int radix;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"+0xfffffffffffffffffffffffffffffffffffffffffffffff", "fffffffffffffffffffffffffffffffffffffffffffffff", 16},
                    {"0xfffffffffffffffffffffffffffffffffffffffffffffff", "fffffffffffffffffffffffffffffffffffffffffffffff", 16},
                    {"-0xfffffffffffffffffffffffffffffffffffffffffffffff", "-fffffffffffffffffffffffffffffffffffffffffffffff", 16},
                    {"+9999999999999999999999999999999999999999999999999", "9999999999999999999999999999999999999999999999999", 10},
                    {"9999999999999999999999999999999999999999999999999", "9999999999999999999999999999999999999999999999999", 10},
                    {"-9999999999999999999999999999999999999999999999999", "-9999999999999999999999999999999999999999999999999", 10},
                    {"+0777777777777777777777777777777777777777777777777", "0777777777777777777777777777777777777777777777777", 8},
                    {"0777777777777777777777777777777777777777777777777", "0777777777777777777777777777777777777777777777777", 8},
                    {"-0777777777777777777777777777777777777777777777777", "-0777777777777777777777777777777777777777777777777", 8}
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.IntegerLiteralContext ctx = parser.integerLiteral();

            assertEquals(new BigInteger(expected, radix), new GenericLiteralVisitor(new GremlinAntlrToJava()).visitIntegerLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidFloatLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public String expected;

        @Parameterized.Parameter(value = 2)
        public String type;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    // default
                    {"1.1", "1.1", "java.lang.Double"},
                    {"-0.1", "-0.1", "java.lang.Double"},
                    {"1.0E+12", "1.0E12", "java.lang.Double"},
                    {"-0.1E-12", "-0.1E-12", "java.lang.Double"},
                    {"1E12", "1E12", "java.lang.Double"},
                    // float
                    {"1.1f", "1.1", "java.lang.Float"},
                    {"-0.1F", "-0.1", "java.lang.Float"},
                    {"1.0E+12f", "1.0E12", "java.lang.Float"},
                    {"-0.1E-12F", "-0.1E-12", "java.lang.Float"},
                    {"1E12f", "1E12", "java.lang.Float"},
                    {"1F", "1", "java.lang.Float"},

                    // double
                    {"1.1d", "1.1", "java.lang.Double"},
                    {"-0.1D", "-0.1", "java.lang.Double"},
                    {"1.0E+12d", "1.0E12", "java.lang.Double"},
                    {"-0.1E-12D", "-0.1E-12", "java.lang.Double"},
                    {"1E12d", "1E12", "java.lang.Double"},
                    {"1D", "1", "java.lang.Double"}
            });
        }

        @Test
        public void shouldParse() throws Exception {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.FloatLiteralContext ctx = parser.floatLiteral();

            final Class<?> clazz = Class.forName(type);
            final Constructor<?> ctor = clazz.getConstructor(String.class);
            final Object expectedValue = ctor.newInstance(expected);

            Object result = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitFloatLiteral(ctx);
            if (expectedValue instanceof Double) {
                assertEquals((Double) expectedValue, (Double) result, 0.00000001);
            } else if (expectedValue instanceof Float) {
                assertEquals((Float) expectedValue, (Float) result, 0.00000001);
            } else {
                assertEquals(expectedValue, result);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidEnumGTypeLiteralTest {

        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public GType expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"bigDecimal", GType.BIGDECIMAL},
                    {"bigInt", GType.BIGINT},
                    {"binary", GType.BINARY},
                    {"boolean", GType.BOOLEAN},
                    {"byte", GType.BYTE},
                    {"char", GType.CHAR},
                    {"datetime", GType.DATETIME},
                    {"double", GType.DOUBLE},
                    {"duration", GType.DURATION},
                    {"edge", GType.EDGE},
                    {"float", GType.FLOAT},
                    {"graph", GType.GRAPH},
                    {"int", GType.INT},
                    {"list", GType.LIST},
                    {"long", GType.LONG},
                    {"map", GType.MAP},
                    {"null", GType.NULL},
                    {"number", GType.NUMBER},
                    {"path", GType.PATH},
                    {"property", GType.PROPERTY},
                    {"set", GType.SET},
                    {"short", GType.SHORT},
                    {"string", GType.STRING},
                    {"tree", GType.TREE},
                    {"UUID", GType.UUID},
                    {"vertex", GType.VERTEX},
                    {"vproperty", GType.VPROPERTY},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.TraversalGTypeContext ctx = parser.traversalGType();

            final GType gType = (GType) new GenericLiteralVisitor(new GremlinAntlrToJava()).visitTraversalGType(ctx);
            assertEquals(expected, gType);
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidDatetimeLiteralTest {

        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public OffsetDateTime expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"datetime('2018-03-22T00:35:44.741Z')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"datetime('2018-03-22T00:35:44.741-0000')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"datetime('2018-03-22T00:35:44.741+0000')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"datetime('2018-03-22T00:35:44.741-0300')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(-3))},
                    {"datetime('2018-03-22T00:35:44.741+1600')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), ZoneOffset.ofHours(16))},
                    {"datetime('2018-03-22T00:35:44.741')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 741000000), UTC)},
                    {"datetime('2018-03-22T00:35:44Z')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 0), UTC)},
                    {"datetime('2018-03-22T00:35:44')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 00, 35, 44, 0), UTC)},
                    {"datetime('2018-03-22')", OffsetDateTime.of(LocalDateTime.of(2018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"datetime('1018-03-22')", OffsetDateTime.of(LocalDateTime.of(1018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"datetime('9018-03-22')", OffsetDateTime.of(LocalDateTime.of(9018, 03, 22, 0, 0, 0, 0), UTC)},
                    {"datetime('1000-001')", OffsetDateTime.of(LocalDateTime.of(1000, 1, 1, 0, 0, 0, 0), UTC)},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DateLiteralContext ctx = parser.dateLiteral();

            final OffsetDateTime dt = (OffsetDateTime) new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDateLiteral(ctx);
            assertEquals(expected, dt);
        }
    }

    public static class ValidCurrentDateLiteralTest {
        @Test
        public void shouldParseCurrentDate() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("datetime()"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DateLiteralContext ctx = parser.dateLiteral();

            final OffsetDateTime dt = (OffsetDateTime) new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDateLiteral(ctx);
            assertTrue(OffsetDateTime.now(UTC).toInstant().toEpochMilli() - dt.toInstant().toEpochMilli() < 1000);
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidBooleanLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public boolean expected;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"true", true},
                    {"false", false}
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.BooleanLiteralContext ctx = parser.booleanLiteral();

            assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitBooleanLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidMapLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public Map<Object, Object> expectedMap;

        @Parameterized.Parameters(name = "{index}: shouldParse({0})")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"[\"name\":\"simba\"]", createMap(new Object[]{"name", "simba"})},
                    {"[(\"name\"):\"simba\"]", createMap(new Object[]{"name", "simba"})},
                    {"[name:\"simba\", age: 29]", createMap(new Object[]{"name", "simba", "age", 29})},
                    {"[:]", createMap(new Object[]{})},
                    {"[1:'a']", createMap(new Object[]{1, "a"})},
                    {"[edges: 'person', T.id: 1]", createMap(new Object[]{"edges", "person", T.id, 1})},
                    {"[(T.label): 'person', T.id: 1]", createMap(new Object[]{T.label, "person", T.id, 1})},
                    {"[T.label: 'person', T.id: 1]", createMap(new Object[]{T.label, "person", T.id, 1})},
                    {"['label': 'person', T.id: 1]", createMap(new Object[]{"label", "person", T.id, 1})},
                    {"[label: 'person', T.id: 1]", createMap(new Object[]{"label", "person", T.id, 1})},
                    {"[(label): 'person', (T.id): 1]", createMap(new Object[]{T.label, "person", T.id, 1})},
                    {"[from: 'source', Direction.to: 'target']", createMap(new Object[]{"from", "source", Direction.to, "target"})},
                    {"[(from): 'source', (Direction.to): 'target']", createMap(new Object[]{Direction.from, "source", Direction.to, "target"})},
                    {"[\"name\":\"simba\",\"age\":32]", createMap(new Object[]{"name", "simba", "age", 32})},
                    {"[\"name\":\"simba\",\"age\":[2,3]]", createMap(new Object[]{"name", "simba", "age", Arrays.asList(2, 3)})},
                    {"[3L:\"32\",([1I, 2I, 3.1D]):4I,\"x\":4I,\"+x\":8I]", createMap(new Object[]{3L, "32", Arrays.asList(1, 2, 3.1), 4, "x", 4, "+x", 8})},
                    {"[[label: 'person', T.id: 1]:\"x\"]", createMap(new Object[]{createMap(new Object[]{"label", "person", T.id, 1}), "x"})},
                    {"[([label: 'person', T.id: 1]):\"x\"]", createMap(new Object[]{createMap(new Object[]{"label", "person", T.id, 1}), "x"})},
                    {"[new: true]", createMap(new Object[]{"new", true})},
                    {"[new : true]", createMap(new Object[]{"new", true})},
                    {"['new' : true]", createMap(new Object[]{"new", true})},
                    {"[s: {'x'}]", createMap(new Object[]{"s", new HashSet<>(Arrays.asList("x"))})},
                    {"[l: ['x']]", createMap(new Object[]{"l", Arrays.asList("x")})},
                    {"[({'x'}): 'x']", createMap(new Object[]{new HashSet<>(Arrays.asList("x")), "x"})},
                    {"[{'x'}: 'x']", createMap(new Object[]{new HashSet<>(Arrays.asList("x")), "x"})},
                    {"[['x']: ['x',{'y'}]]", createMap(new Object[]{Arrays.asList("x"), Arrays.asList("x", new HashSet<>(Arrays.asList("y")))})},
                    {"[['x']: ['x',['y']]]", createMap(new Object[]{Arrays.asList("x"), Arrays.asList("x", Arrays.asList("y"))})},
                    {"[null:'x']", createMap(new Object[]{null, "x"})},
                    {"[null:null]", createMap(new Object[]{null, null})},
                    {"[null:'x','y':'z']", createMap(new Object[]{null, "x", "y", "z"})},
                    {"['null':'x']", createMap(new Object[]{"null", "x"})},
            });
        }

        private static Map<Object, Object> createMap(final Object[] keyValuePairs) {
            final Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                map.put(keyValuePairs[i], keyValuePairs[i + 1]);
            }
            return map;
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Map
            assertThat(script, genericLiteral, instanceOf(Map.class));

            // verify the parsed map matches the expected map
            final Map<Object, Object> genericLiterals = (Map<Object, Object>) genericLiteral;
            assertEquals(expectedMap, genericLiterals);
        }
    }

    public static class ValidListLiteralTest {

        @Test
        public void shouldParseGenericLiteralCollection() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("['world', 165, 165, [12L, 0xA, 14.5, \"hello\"]]"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Object[]
            assertThat(genericLiteral, instanceOf(List.class));

            // verify total number of elements
            List<Object> genericLiterals = (List<Object>) genericLiteral;
            assertEquals(genericLiterals.size(), 4);

            // verify first element
            assertEquals("world", genericLiterals.get(0));

            // verify second/third element
            assertEquals(165, genericLiterals.get(1));
            assertEquals(165, genericLiterals.get(2));

            // verify 4th element
            assertThat(genericLiterals.get(3), instanceOf(List.class));

            // verify total number of elements
            genericLiterals = (List<Object>) genericLiterals.get(3);
            assertEquals(genericLiterals.size(), 4);

            // verify first element
            assertEquals(12L, genericLiterals.get(0));

            // verify second element
            assertEquals(10, genericLiterals.get(1));

            // verify 3rd element
            assertEquals(new Double(14.5), genericLiterals.get(2));

            // verify 4th element
            assertEquals("hello", genericLiterals.get(3));
        }

        /**
         * Generic literal collection test
         */
        @Test
        public void shouldParseEmptyGenericLiteralCollection() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("[]"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Object[]
            assertThat(genericLiteral, instanceOf(List.class));

            // verify total number of elements
            final List<Object> genericLiterals = (List<Object>) genericLiteral;
            Assert.assertTrue(genericLiterals.isEmpty());
        }
    }

    public static class ValidSetLiteralTest {

        @Test
        public void shouldParseGenericLiteralSet() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("{'world', 165, 14.5, {12L, 0xA, 14.5, \"hello\"}}"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Object[]
            assertThat(genericLiteral, instanceOf(Set.class));

            // verify total number of elements
            Set<Object> genericLiterals = (Set<Object>) genericLiteral;
            assertEquals(genericLiterals.size(), 4);

            assertThat(genericLiterals.contains("world"), Matchers.is(true));
            assertThat(genericLiterals.contains(165), Matchers.is(true));
            assertThat(genericLiterals.contains(14.5), Matchers.is(true));
            assertThat(genericLiterals.contains(new HashSet<Object>() {{
                add(12L);
                add(10);
                add(14.5);
                add("hello");
            }}), Matchers.is(true));
        }

        @Test
        public void shouldParseGenericLiteralSetWithEmbeddedList() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("{'world', 165, 14.5, [12L, 0xA, 14.5, \"hello\"]}"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Object[]
            assertThat(genericLiteral, instanceOf(Set.class));

            // verify total number of elements
            Set<Object> genericLiterals = (Set<Object>) genericLiteral;
            assertEquals(genericLiterals.size(), 4);

            assertThat(genericLiterals.contains("world"), Matchers.is(true));
            assertThat(genericLiterals.contains(165), Matchers.is(true));
            assertThat(genericLiterals.contains(14.5), Matchers.is(true));
            assertThat(genericLiterals.contains(new ArrayList<Object>() {{
                add(12L);
                add(10);
                add(14.5);
                add("hello");
            }}), Matchers.is(true));
        }

        /**
         * Generic literal collection test
         */
        @Test
        public void shouldParseEmptyGenericLiteralCollection() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("{}"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.GenericLiteralContext ctx = parser.genericLiteral();
            final Object genericLiteral = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitGenericLiteral(ctx);

            // verify type is Object[]
            assertThat(genericLiteral, instanceOf(Set.class));

            // verify total number of elements
            final Set<Object> genericLiterals = (Set<Object>) genericLiteral;
            Assert.assertTrue(genericLiterals.isEmpty());
        }
    }

    public static class NullNaNInfTest {

        @Test
        public void shouldParseNull() {
            final String script = "null";

            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.NullLiteralContext ctx = parser.nullLiteral();
            assertEquals(null, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitNullLiteral(ctx));
        }

        @Test
        public void shouldParseNaN() {
            final String script = "NaN";

            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.NanLiteralContext ctx = parser.nanLiteral();

            final Object o = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitNanLiteral(ctx);
            assertTrue(o instanceof Double);
            assertTrue(Double.isNaN((double) o));
        }

        @Test
        public void shouldParseInf() {
            final String script = "Infinity";

            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.InfLiteralContext ctx = parser.infLiteral();

            assertEquals(Double.POSITIVE_INFINITY, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitInfLiteral(ctx));
        }

        @Test
        public void shouldParsePosInf() {
            final String script = "+Infinity";

            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.InfLiteralContext ctx = parser.infLiteral();

            assertEquals(Double.POSITIVE_INFINITY, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitInfLiteral(ctx));
        }

        @Test
        public void shouldParseNegInf() {
            final String script = "-Infinity";

            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.InfLiteralContext ctx = parser.infLiteral();

            assertEquals(Double.NEGATIVE_INFINITY, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitInfLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class CardinalityTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public Object expected;

        @Parameterized.Parameters()
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"single(\"test\")", VertexProperty.Cardinality.single("test")},
                    {"list(\"test\")", VertexProperty.Cardinality.list("test")},
                    {"set(\"test\")", VertexProperty.Cardinality.set("test")},
                    {"Cardinality.single(\"test\")", VertexProperty.Cardinality.single("test")},
                    {"Cardinality.list(\"test\")", VertexProperty.Cardinality.list("test")},
                    {"Cardinality.set(\"test\")", VertexProperty.Cardinality.set("test")},
                    {"single(1l)", VertexProperty.Cardinality.single(1L)},
                    {"list(1l)", VertexProperty.Cardinality.list(1L)},
                    {"set(1l)", VertexProperty.Cardinality.set(1L)},
                    {"Cardinality.single", VertexProperty.Cardinality.single},
                    {"Cardinality.list", VertexProperty.Cardinality.list},
                    {"Cardinality.set", VertexProperty.Cardinality.set},
                    {"single", VertexProperty.Cardinality.single},
                    {"list", VertexProperty.Cardinality.list},
                    {"set", VertexProperty.Cardinality.set},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.TraversalCardinalityContext ctx = parser.traversalCardinality();
            assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitTraversalCardinality(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidCharacterLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public Character expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"\"a\"c", 'a'},
                    {"'a'c", 'a'},
                    {"\"\\\"\"c", '"'},
                    {"'\\''c", '\''},
                    {"\"\\\\\"c", '\\'},
                    {"\"'\"c", '\''},
                    {"\"\\u00E9\"c", '\u00E9'},
                    {"'\u00E9'c", '\u00E9'},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.CharacterLiteralContext ctx = parser.characterLiteral();
            assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitCharacterLiteral(ctx));
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidDurationLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public Duration expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"Duration(9000, 0)", Duration.ofHours(2).plusMinutes(30)},
                    {"Duration(0, 0)", Duration.ZERO},
                    {"Duration(30, 0)", Duration.ofSeconds(30)},
                    {"Duration(0, 500000000)", Duration.ofMillis(500)},
                    {"Duration(5415, 0)", Duration.ofHours(1).plusMinutes(30).plusSeconds(15)},
                    {"Duration(0, 1)", Duration.ofNanos(1)},
                    {"Duration(1, 500000000)", Duration.ofSeconds(1).plusMillis(500)},
                    {"Duration(30, 0, false)", Duration.ofSeconds(-30)},
                    {"Duration(0, 500000000, false)", Duration.ofMillis(-500)},
                    {"Duration(0, 0, false)", Duration.ZERO},
                    {"Duration(0, 0, true)", Duration.ZERO},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DurationLiteralContext ctx = parser.durationLiteral();
            assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDurationLiteral(ctx));
        }
    }

    public static class InvalidDurationLiteralTest {
        @Test
        public void shouldFailOnNegativeNanos() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("Duration(0, -1)"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DurationLiteralContext ctx = parser.durationLiteral();
            try {
                new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDurationLiteral(ctx);
                fail("Negative nanos should have thrown exception");
            } catch (GremlinParserException gpe) {
                assertThat(gpe.getMessage().contains("nanoseconds must be between 0 and 999999999"), Matchers.is(true));
            }
        }

        @Test
        public void shouldFailOnNanosOverflow() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("Duration(0, 1000000000)"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DurationLiteralContext ctx = parser.durationLiteral();
            try {
                new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDurationLiteral(ctx);
                fail("Nanos overflow should have thrown exception");
            } catch (GremlinParserException gpe) {
                assertThat(gpe.getMessage().contains("nanoseconds must be between 0 and 999999999"), Matchers.is(true));
            }
        }

        @Test
        public void shouldFailOnNegativeSeconds() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("Duration(-1, 0)"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.DurationLiteralContext ctx = parser.durationLiteral();
            try {
                new GenericLiteralVisitor(new GremlinAntlrToJava()).visitDurationLiteral(ctx);
                fail("Negative seconds should have thrown exception");
            } catch (GremlinParserException gpe) {
                assertThat(gpe.getMessage().contains("seconds must be non-negative"), Matchers.is(true));
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class ValidBinaryLiteralTest {
        @Parameterized.Parameter(value = 0)
        public String script;

        @Parameterized.Parameter(value = 1)
        public ByteBuffer expected;

        @Parameterized.Parameters(name = "{0}")
        public static Iterable<Object[]> generateTestParameters() {
            return Arrays.asList(new Object[][]{
                    {"Binary(\"AQID\")", ByteBuffer.wrap(new byte[]{1, 2, 3})},
                    {"Binary(\"\")", ByteBuffer.wrap(new byte[]{})},
                    {"Binary(\"AA==\")", ByteBuffer.wrap(new byte[]{0})},
            });
        }

        @Test
        public void shouldParse() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.BinaryLiteralContext ctx = parser.binaryLiteral();
            assertEquals(expected, new GenericLiteralVisitor(new GremlinAntlrToJava()).visitBinaryLiteral(ctx));
        }
    }

    public static class InvalidBinaryLiteralTest {
        @Test
        public void shouldFailOnInvalidBase64() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("Binary(\"!!!not-base64\")"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.BinaryLiteralContext ctx = parser.binaryLiteral();
            try {
                new GenericLiteralVisitor(new GremlinAntlrToJava()).visitBinaryLiteral(ctx);
                fail("Invalid Binary/base64 value should have thrown exception");
            } catch (GremlinParserException gpe) {
                assertThat(gpe.getMessage().contains("Invalid Binary literal:"), Matchers.is(true));
            }
        }
    }

    public static class ValidPdtLiteralTest {
        @Test
        public void shouldParsePdtLiteral() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("PDT(\"MyType\",[\"x\":1,\"y\":\"hello\"])"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.PdtLiteralContext ctx = parser.pdtLiteral();
            final Object result = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitPdtLiteral(ctx);
            assertThat(result, instanceOf(ProviderDefinedType.class));
            final ProviderDefinedType pdt = (ProviderDefinedType) result;
            assertEquals("MyType", pdt.getName());
            assertEquals(1, pdt.getFields().get("x"));
            assertEquals("hello", pdt.getFields().get("y"));
        }

        @Test
        public void shouldParsePdtLiteralWithEmptyMap() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("PDT(\"Empty\",[:])"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.PdtLiteralContext ctx = parser.pdtLiteral();
            final Object result = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitPdtLiteral(ctx);
            assertThat(result, instanceOf(ProviderDefinedType.class));
            final ProviderDefinedType pdt = (ProviderDefinedType) result;
            assertEquals("Empty", pdt.getName());
            assertTrue(pdt.getFields().isEmpty());
        }

        @Test
        public void shouldRejectNonStringMapKey() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("PDT(\"Bad\",[1:\"value\"])"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.PdtLiteralContext ctx = parser.pdtLiteral();
            try {
                new GenericLiteralVisitor(new GremlinAntlrToJava()).visitPdtLiteral(ctx);
                fail("Expected IllegalArgumentException for non-String map key");
            } catch (final IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("PDT fields map must have String keys, found: java.lang.Integer"));
            }
        }

        @Test
        public void shouldParsePrimitivePdtLiteral() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("PDT(\"Uint32\",\"42\")"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.PdtLiteralContext ctx = parser.pdtLiteral();
            final Object result = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitPdtLiteral(ctx);
            assertThat(result, instanceOf(PrimitiveProviderDefinedType.class));
            final PrimitiveProviderDefinedType pdt = (PrimitiveProviderDefinedType) result;
            assertEquals("Uint32", pdt.getName());
            assertEquals("42", pdt.getValue());
        }

        @Test
        public void shouldParsePrimitivePdtLiteralWithEmptyValue() {
            final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString("PDT(\"Empty\",\"\")"));
            final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
            final GremlinParser.PdtLiteralContext ctx = parser.pdtLiteral();
            final Object result = new GenericLiteralVisitor(new GremlinAntlrToJava()).visitPdtLiteral(ctx);
            assertThat(result, instanceOf(PrimitiveProviderDefinedType.class));
            final PrimitiveProviderDefinedType pdt = (PrimitiveProviderDefinedType) result;
            assertEquals("Empty", pdt.getName());
            assertEquals("", pdt.getValue());
        }
    }
}
