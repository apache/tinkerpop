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
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ArgumentVisitorTest {
    @Parameterized.Parameter(value = 0)
    public Class<?> clazz;

    @Parameterized.Parameter(value = 1)
    public String script;

    @Parameterized.Parameter(value = 2)
    public Object expected;

    @Parameterized.Parameter(value = 3)
    public GremlinAntlrToJava antlrToLanguage;

    private static final GraphTraversalSource g = EmptyGraph.instance().traversal();

    private static final Date now = new Date();

    private static final Map<String, Object> nullMap = new HashMap<String, Object>() {{
        put("x", null);
    }};

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {Boolean.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Boolean.class, "true", true, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", true)))},
                {Boolean.class, "false", false, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", true)))},
                {Boolean.class, "x", true, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", true)))},
                {Integer.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Integer.class, "0", 0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "0i", 0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "0L", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "x", 0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0)))},
                {Integer.class, "x", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0L)))},
                {Float.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Float.class, "0.0d", 0.0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0)))},
                {Float.class, "0d", 0.0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0)))},
                {Float.class, "0F", 0.0F, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0F)))},
                {Float.class, "x", 0.0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0.0)))},
                {Float.class, "x", 0.0F, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0.0F)))},
                {String.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {String.class, "'test'", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {String.class, "x", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "test")))},
                {String.class, "x", "graphson", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", IO.graphson)))},
                {StringNullable.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {StringNullable.class, "null", null, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {StringNullable.class, "x", null, createAntlr(new VariableResolver.DefaultVariableResolver(nullMap))},
                {Object.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Object.class, "'test'", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Object.class, "x", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "test")))},
                {Object.class, "x", now, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", now)))},
                {Object.class, "[1,2,3]", Arrays.asList(1, 2, 3), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Object.class, "x", P.eq(100), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", P.eq(100))))},
                {Vertex.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Vertex.class, "new Vertex(1i,'person')", new ReferenceVertex(1, "person"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", Direction.from)))},
                {Vertex.class, "x", new ReferenceVertex(1, "person"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", new ReferenceVertex(1, "person"))))},
        });
    }

    @Test
    public void shouldParse() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        if (clazz.equals(Boolean.class)) {
            assertParsing(() -> {
                final GremlinParser.BooleanArgumentContext ctx = parser.booleanArgument();
                return antlrToLanguage.argumentVisitor.parseBoolean(ctx);
            });
        } else if (clazz.equals(Integer.class)) {
            assertParsing(() -> {
                final GremlinParser.IntegerArgumentContext ctx = parser.integerArgument();
                return antlrToLanguage.argumentVisitor.parseNumber(ctx);
            });
        } else if (clazz.equals(Float.class)) {
            assertParsing(() -> {
                final GremlinParser.FloatArgumentContext ctx = parser.floatArgument();
                return antlrToLanguage.argumentVisitor.parseNumber(ctx);
            });
        } else if (clazz.equals(String.class)) {
            assertParsing(() -> {
                final GremlinParser.StringArgumentContext ctx = parser.stringArgument();
                return antlrToLanguage.argumentVisitor.parseString(ctx);
            });
        } else if (clazz.equals(StringNullable.class)) {
            assertParsing(() -> {
                final GremlinParser.StringNullableArgumentContext ctx = parser.stringNullableArgument();
                return antlrToLanguage.argumentVisitor.parseString(ctx);
            });
        } else if (clazz.equals(Object.class)) {
            assertParsing(() -> {
                final GremlinParser.GenericLiteralArgumentContext ctx = parser.genericLiteralArgument();
                return antlrToLanguage.argumentVisitor.parseObject(ctx);
            });
        } else if (clazz.equals(List.class)) {
            assertParsing(() -> {
                final GremlinParser.GenericLiteralListArgumentContext ctx = parser.genericLiteralListArgument();
                return antlrToLanguage.argumentVisitor.parseObjectVarargs(ctx);
            });
        } else if (clazz.equals(Vertex.class)) {
            assertParsing(() -> {
                final GremlinParser.StructureVertexArgumentContext ctx = parser.structureVertexArgument();
                return antlrToLanguage.argumentVisitor.parseVertex(ctx);
            });
        } else {
            fail("Missing an assertion type: " + clazz.getSimpleName());
        }
    }

    private void assertParsing(final Supplier<Object> visit) {
        try {
            final Object o = visit.get();
            if (expected instanceof VariableResolverException)
                fail(String.format("Should have failed with %s", VariableResolverException.class.getSimpleName()));
            else
                assertEquals(expected, o);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(VariableResolverException.class));
        }
    }

    private static GremlinAntlrToJava createAntlr(final VariableResolver resolver) {
        return new GremlinAntlrToJava("g", EmptyGraph.instance(), __::start, g, resolver);
    }

    private static class StringNullable { }
}
