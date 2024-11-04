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
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TraversalSourceSelfMethodVisitorTest {
    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Parameterized.Parameter(value = 0)
    public String script;

    @Parameterized.Parameter(value = 1)
    public GraphTraversalSource expected;

    @Parameterized.Parameters()
    public static Iterable<Object[]> generateTestParameters() {
        final HashMap<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        return Arrays.asList(new Object[][]{
                {"withBulk(false)", g.withBulk(false)},
                {"withBulk(true)", g.withBulk(true)},
                {"withPath()", g.withPath()},
                {"withSack('hello')", g.withSack("hello")},
                {"withSack('hello', addAll)", g.withSack("hello", Operator.addAll)},
                {"withSideEffect('hello', 12)", g.withSideEffect("hello", 12)},
                {"withSideEffect('hello', 12, sum)", g.withSideEffect("hello", 12, Operator.sum)},
                {"withStrategies(ReadOnlyStrategy)", g.withStrategies(ReadOnlyStrategy.instance())},
                {"withStrategies(new EdgeLabelVerificationStrategy(logWarning: true, throwException: true))", g.withStrategies(EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create())},
                {"withStrategies(EdgeLabelVerificationStrategy(logWarning: true, throwException: true))", g.withStrategies(EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create())},
                {"withStrategies(ReadOnlyStrategy, EdgeLabelVerificationStrategy(logWarning: true, throwException: true))", g.withStrategies(ReadOnlyStrategy.instance(), EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create())},
                {"withStrategies(ReadOnlyStrategy, new EdgeLabelVerificationStrategy(logWarning: true, throwException: true))", g.withStrategies(ReadOnlyStrategy.instance(), EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create())},
                {"withStrategies(new EdgeLabelVerificationStrategy(logWarning: true, throwException: true), ReadOnlyStrategy)", g.withStrategies(EdgeLabelVerificationStrategy.build().logWarning(true).throwException(true).create(), ReadOnlyStrategy.instance())},
                {"withoutStrategies(CountStrategy)", g.withoutStrategies(CountStrategy.class)},
                {"withoutStrategies(CountStrategy, EarlyLimitStrategy)", g.withoutStrategies(CountStrategy.class, EarlyLimitStrategy.class)},
                {"withoutStrategies(CountStrategy, EarlyLimitStrategy, PathRetractionStrategy)", g.withoutStrategies(CountStrategy.class, EarlyLimitStrategy.class, PathRetractionStrategy.class)},
                {"withoutStrategies(CountStrategy, EarlyLimitStrategy, PathRetractionStrategy, RepeatUnrollStrategy)", g.withoutStrategies(CountStrategy.class, EarlyLimitStrategy.class, PathRetractionStrategy.class, RepeatUnrollStrategy.class)},
                {"with('requestId', '7c55d4d7-809a-4f84-9720-63b48cb2fd14')", g.with("requestId", "7c55d4d7-809a-4f84-9720-63b48cb2fd14")},
                {"with('requestId')", g.with("requestId")},
                {"withSideEffect('hello', ['one':1])", g.withSideEffect("hello", map)},
                {"withSideEffect('hello', ['one' : 1])", g.withSideEffect("hello", map)},
                {"withSideEffect('hello', ['one' : 1], Operator.addAll)", g.withSideEffect("hello", map, Operator.addAll)},
                {"withSideEffect('hello', ['one':1, 'two':2])", g.withSideEffect("hello", new HashMap<String, Integer>() {{
                    put("one", 1);
                    put("two", 2);
                }})}
        });
    }

    @Test
    public void shouldParseTraversalSourceSelfMethod() {
        final GremlinLexer lexer = new GremlinLexer(CharStreams.fromString(script));
        final GremlinParser parser = new GremlinParser(new CommonTokenStream(lexer));
        final GremlinParser.TraversalSourceSelfMethodContext ctx = parser.traversalSourceSelfMethod();
        final GraphTraversalSource source = new TraversalSourceSelfMethodVisitor(g, new GremlinAntlrToJava()).visitTraversalSourceSelfMethod(ctx);

        assertEquals(expected.getGremlinLang(), source.getGremlinLang());
    }
}
