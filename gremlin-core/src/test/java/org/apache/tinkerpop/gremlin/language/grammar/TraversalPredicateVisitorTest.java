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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TraversalPredicateVisitorTest {

    @Parameterized.Parameter(value = 0)
    public String script;

    @Parameterized.Parameter(value = 1)
    public P expected;

    @Parameterized.Parameters()
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {"eq(10)", P.eq(10)},
                {"neq(1.0f)", P.neq(1.0f)},
                {"lt(0x11)", P.lt(0x11)},
                {"lte('abc')", P.lte("abc")},
                {"gt(1.0D)", P.gt(1d)},
                {"gte(1L)", P.gte(1L)},
                {"inside(100, 200)", P.inside(100, 200)},
                {"outside(1E11, 2e-11)", P.outside(new BigDecimal("1E11"), new BigDecimal("2e-11"))},
                {"between(\"a\", \"e\")", P.between("a", "e")},
                {"within([\"a\", \"e\"])", P.within(Arrays.asList("a", "e"))},
                {"within()", P.within()},
                {"without([1, 2])", P.without(Arrays.asList(1, 2))},
                {"without([1, 2])", P.without(1, 2)},
                {"without(1, 2)", P.without(1, 2)},
                {"without(1)", P.without(1)},
                {"without([1])", P.without(1)},
                {"without()", P.without()},
                {"without(1, [1, 2])", P.without(1, Arrays.asList(1, 2))},
                {"within([1, 2, 3], 1, [1, 2])", P.within(Arrays.asList(1, 2, 3), 1, Arrays.asList(1, 2))},
                {"not(without(1, 2))", P.not(P.without(1, 2))},
                {"not(eq(10))", P.not(P.eq(10))},
                {"not(eq(10)).and(not(eq(11)))", P.not(P.eq(10)).and(P.not(P.eq(11)))},
                {"not(eq(10)).or(not(eq(11)))", P.not(P.eq(10)).or(P.not(P.eq(11)))},
                {"not(eq(10)).negate()", P.not(P.eq(10)).negate()},
                {"P.eq(10)", P.eq(10)},
                {"P.neq(1.0f)", P.neq(1.0f)},
                {"P.lt(0x11)", P.lt(0x11)},
                {"P.lte('abc')", P.lte("abc")},
                {"P.gt(1.0D)", P.gt(1d)},
                {"P.gte(1L)", P.gte(1L)},
                {"P.inside(100, 200)", P.inside(100, 200)},
                {"P.outside(1E11, 2e-11)", P.outside(new BigDecimal("1E11"), new BigDecimal("2e-11"))},
                {"P.between(\"a\", \"e\")", P.between("a", "e")},
                {"P.within([\"a\", \"e\"])", P.within(Arrays.asList("a", "e"))},
                {"P.within()", P.within()},
                {"P.without()", P.without()},
                {"P.without([1, 2])", P.without(Arrays.asList(1, 2))},
                {"P.not(P.without(1, 2))", P.not(P.without(1, 2))},
                {"P.not(P.eq(10))", P.not(P.eq(10))},
                {"P.not(eq(10)).and(P.not(eq(11)))", P.not(P.eq(10)).and(P.not(P.eq(11)))},
                {"P.not(P.eq(10)).or(P.not(P.eq(11)))", P.not(P.eq(10)).or(P.not(P.eq(11)))},
                {"P.not(P.eq(10)).negate()", P.not(P.eq(10)).negate()},
                {"TextP.containing('hakuna').negate()", TextP.containing("hakuna").negate()},
                {"TextP.notContaining('hakuna')", TextP.notContaining("hakuna")},
                {"TextP.startingWith('hakuna')", TextP.startingWith("hakuna")},
                {"TextP.endingWith('hakuna')", TextP.endingWith("hakuna")},
                {"TextP.notEndingWith('hakuna')", TextP.notEndingWith("hakuna")},
                {"TextP.notStartingWith('hakuna')", TextP.notStartingWith("hakuna")},
                {"TextP.regex('^h')", TextP.regex("^h")},
                {"TextP.regex('~h')", TextP.regex("~h")},
                {"TextP.regex('(?i)gfg')", TextP.regex("(?i)gfg")},
                {"TextP.notRegex('^h')", TextP.notRegex("^h")},
                {"TextP.regex('^h').negate()", TextP.regex("^h").negate()},
        });
    }

    @Test
    public void testPredicate() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.TraversalPredicateContext ctx = parser.traversalPredicate();
        final P predicate = TraversalPredicateVisitor.instance().visitTraversalPredicate(ctx);

        Assert.assertEquals(expected, predicate);
    }
}
