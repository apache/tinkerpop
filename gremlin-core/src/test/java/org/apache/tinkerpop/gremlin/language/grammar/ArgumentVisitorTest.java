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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
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
                {Boolean.class, "x", GValue.of("x", true), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", true)))},
                {Boolean.class, "x", true, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", true)))},
                {Integer.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Integer.class, "0", 0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "0i", 0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "0L", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Integer.class, "x", GValue.of("x", 0), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0)))},
                {Integer.class, "x", GValue.of("x", 0L), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0L)))},
                {Integer.class, "x", 0, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0)))},
                {Integer.class, "x", 0L, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0L)))},
                {Long.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Long.class, "0", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Long.class, "0i", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Long.class, "0L", 0L, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 100)))},
                {Long.class, "x", GValue.ofLong("x", 0L), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", (byte) 0)))},
                {Long.class, "x", GValue.ofLong("x", 0L), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", (short) 0)))},
                {Long.class, "x", GValue.ofLong("x", 0L), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0)))},
                {Long.class, "x", GValue.ofLong("x", 0L), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0L)))},
                {Long.class, "x", 0L, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", (byte) 0)))},
                {Long.class, "x", 0L, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", (short) 0)))},
                {Long.class, "x", 0L, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0)))},
                {Long.class, "x", 0L, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0L)))},
                {Float.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Float.class, "0.0d", 0.0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0)))},
                {Float.class, "0d", 0.0, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0)))},
                {Float.class, "0F", 0.0F, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 1000.0F)))},
                {Float.class, "x", GValue.of("x", 0.0), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0.0)))},
                {Float.class, "x", GValue.of("x", 0.0F), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", 0.0F)))},
                {Float.class, "x", 0.0, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0.0)))},
                {Float.class, "x", 0.0F, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", 0.0F)))},
                {String.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {String.class, "'test'", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {String.class, "x", GValue.of("x", "test"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "test")))},
                {String.class, "x", GValue.of("x", "graphson"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", IO.graphson)))},
                {String.class, "x", "test", createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", "test")))},
                {String.class, "x", "graphson", createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", IO.graphson)))},
                {StringNullable.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {StringNullable.class, "null", null, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {StringNullable.class, "x", GValue.of("x", null), createAntlr(new VariableResolver.DefaultVariableResolver(nullMap))},
                {StringNullable.class, "x", null, createAntlr(new VariableResolver.DirectVariableResolver(nullMap))},
                {Object.class, "x", new VariableResolverException("x"), createAntlr(VariableResolver.NoVariableResolver.instance())},
                {Object.class, "'test'", "test", createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Object.class, "x", GValue.of("x", "test"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "test")))},
                {Object.class, "x", GValue.of("x", now), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", now)))},
                {Object.class, "x", "test", createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", "test")))},
                {Object.class, "x", now, createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", now)))},
                {Object.class, "[1,2,3]", Arrays.asList(1, 2, 3), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", now)))},
                {Object.class, "x", GValue.of("x", P.eq(100)), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", P.eq(100))))},
                {Object.class, "x", P.eq(100), createAntlr(new VariableResolver.DirectVariableResolver(ElementHelper.asMap("x", P.eq(100))))},
                {Direction.class, "Direction.OUT", Direction.OUT, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Direction.class, "OUT", Direction.OUT, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Vertex.class, "new Vertex(1i,'person')", new ReferenceVertex(1, "person"), createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", Direction.from)))},
                {Order.class, "Order.desc", Order.desc, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Scope.class, "Scope.local", Scope.local, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Scope.class, "local", Scope.local, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {T.class, "T.label", T.label, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {T.class, "label", T.label, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {VertexProperty.Cardinality.class, "Cardinality.list", VertexProperty.Cardinality.list, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {VertexProperty.Cardinality.class, "list", VertexProperty.Cardinality.list, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {DT.class, "DT.hour", DT.hour, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {DT.class, "hour", DT.hour, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Merge.class, "Merge.onMatch", Merge.onMatch, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Merge.class, "onMatch", Merge.onMatch, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Pop.class, "Pop.last", Pop.last, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Pop.class, "last", Pop.last, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Operator.class, "Operator.sum", Operator.sum, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Operator.class, "sum", Operator.sum, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Column.class, "Column.keys", Column.keys, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {Column.class, "keys", Column.keys, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {SackFunctions.Barrier.class, "Barrier.normSack", SackFunctions.Barrier.normSack, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
                {SackFunctions.Barrier.class, "Barrier.normSack", SackFunctions.Barrier.normSack, createAntlr(new VariableResolver.DefaultVariableResolver(ElementHelper.asMap("x", "nope")))},
        });
    }

    @Test
    public void shouldParse() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        if (clazz.equals(Boolean.class)) {
            assertParsing(() -> {
                final GremlinParser.BooleanArgumentContext ctx = parser.booleanArgument();
                return antlrToLanguage.argumentVisitor.visitBooleanArgument(ctx);
            });
        } else if (clazz.equals(Integer.class)) {
            assertParsing(() -> {
                final GremlinParser.IntegerArgumentContext ctx = parser.integerArgument();
                return antlrToLanguage.argumentVisitor.visitIntegerArgument(ctx);
            });
        } else if (clazz.equals(Long.class)) {
            assertParsing(() -> {
                final GremlinParser.IntegerArgumentContext ctx = parser.integerArgument();
                return antlrToLanguage.argumentVisitor.parseLong(ctx);
            });
        } else if (clazz.equals(Float.class)) {
            assertParsing(() -> {
                final GremlinParser.FloatArgumentContext ctx = parser.floatArgument();
                return antlrToLanguage.argumentVisitor.visitFloatArgument(ctx);
            });
        } else if (clazz.equals(String.class)) {
            assertParsing(() -> {
                final GremlinParser.StringArgumentContext ctx = parser.stringArgument();
                return antlrToLanguage.argumentVisitor.visitStringArgument(ctx);
            });
        } else if (clazz.equals(StringNullable.class)) {
            assertParsing(() -> {
                final GremlinParser.StringNullableArgumentContext ctx = parser.stringNullableArgument();
                return antlrToLanguage.argumentVisitor.visitStringNullableArgument(ctx);
            });
        } else if (clazz.equals(Object.class)) {
            assertParsing(() -> {
                final GremlinParser.GenericLiteralArgumentContext ctx = parser.genericLiteralArgument();
                return antlrToLanguage.argumentVisitor.visitGenericLiteralArgument(ctx);
            });
        } else if (clazz.equals(List.class)) {
            assertParsing(() -> {
                final GremlinParser.GenericLiteralListArgumentContext ctx = parser.genericLiteralListArgument();
                return antlrToLanguage.argumentVisitor.parseObjectVarargs(ctx);
            });
        } else if (clazz.equals(Direction.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, parser.traversalDirection()));
        } else if (clazz.equals(Vertex.class)) {
            assertParsing(() -> {
                final GremlinParser.StructureVertexArgumentContext ctx = parser.structureVertexArgument();
                return antlrToLanguage.argumentVisitor.visitStructureVertexArgument(ctx);
            });
        } else if (clazz.equals(Order.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Order.class, parser.traversalOrder()));
        } else if (clazz.equals(Scope.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, parser.traversalScope()));
        } else if (clazz.equals(T.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(T.class, parser.traversalToken()));
        } else if (clazz.equals(VertexProperty.Cardinality.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(VertexProperty.Cardinality.class, parser.traversalCardinality()));
        } else if (clazz.equals(DT.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(DT.class, parser.traversalDT()));
        } else if (clazz.equals(Merge.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, parser.traversalMerge()));
        } else if (clazz.equals(Pop.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, parser.traversalPop()));
        } else if (clazz.equals(Operator.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, parser.traversalOperator()));
        } else if (clazz.equals(Column.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(Column.class, parser.traversalColumn()));
        } else if (clazz.equals(SackFunctions.Barrier.class)) {
            assertParsing(() -> TraversalEnumParser.parseTraversalEnumFromContext(SackFunctions.Barrier.class, parser.traversalBarrier()));
        } else {
            fail("Missing an assertion type");
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