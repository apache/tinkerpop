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
package org.apache.tinkerpop.gremlin.tinkergraph.jsr223;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.VariableResolverCustomizer;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.GValueReductionStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * Concrete testing of {@link GremlinLangScriptEngine} evaluations, particularly around its traversal cache.
 */
@RunWith(Parameterized.class)
public class TinkerGraphGremlinLangScriptEngineTest {

    @Parameterized.Parameter(value = 0)
    public String gremlinScript;

    @Parameterized.Parameter(value = 1)
    public List<Pair<Bindings, List<Object>>> bindingsAndResults;

    @Parameterized.Parameter(value = 2)
    public TinkerGraph graph;

    private GremlinLangScriptEngine scriptEngine;
    private GraphTraversalSource g;

    /**
     * Use {@link GValue} instance when resolving variables in the parser.
     */
    private final VariableResolverCustomizer variableResolverCustomizer = new VariableResolverCustomizer(
            VariableResolver.DefaultVariableResolver::new);

    /**
     * Enable caching
     */
    private final GremlinLangCustomizer gremlinLangCustomizer = new GremlinLangCustomizer(true, Caffeine.newBuilder());

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {
        return Arrays.asList(new Object[][]{
                {
                    "g.V().limit(x).count()",
                    Arrays.asList(
                        Pair.of(createBindings("x", 1L), Arrays.asList(1L)),
                        Pair.of(createBindings("x", 2L), Arrays.asList(2L)),
                        Pair.of(createBindings("x", 4L), Arrays.asList(4L))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).values('age')",
                    Arrays.asList(
                        Pair.of(createBindings("x", "vadas"), Arrays.asList(27)),
                        Pair.of(createBindings("x", "marko"), Arrays.asList(29))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).out(y).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "josh", "y", "created"), Arrays.asList("lop" ,"ripple")),
                        Pair.of(createBindings("x", "marko", "y", "knows"), Arrays.asList("josh", "vadas"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).in(y).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "lop", "y", "created"), Arrays.asList("josh" ,"marko")),
                        Pair.of(createBindings("x", "vadas", "y", "knows"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).both(y,z).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "marko", "y", "created", "z", "knows"), Arrays.asList("josh" ,"lop", "vadas")),
                        Pair.of(createBindings("x", "vadas", "y", "knows", "z", "created"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('age', gt(x)).values('name')",
                    Arrays.asList(
                        Pair.of(createBindings("x", 31), Arrays.asList("josh" ,"peter")),
                        Pair.of(createBindings("x", 34), Arrays.asList("peter"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('age', gt(x).and(lt(y))).values('name')",
                    Arrays.asList(
                        Pair.of(createBindings("x", 26, "y", 30), Arrays.asList("marko" ,"vadas")),
                        Pair.of(createBindings("x", 26, "y", 29), Arrays.asList("vadas"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.V().has('name', x).union(both(y), both(z)).values('name').order().by(asc)",
                    Arrays.asList(
                        Pair.of(createBindings("x", "marko", "y", "created", "z", "knows"), Arrays.asList("josh" ,"lop", "vadas")),
                        Pair.of(createBindings("x", "vadas", "y", "knows", "z", "created"), Arrays.asList("marko"))
                    ),
                    TinkerFactory.createModern()
                },
                {
                    "g.addV(x).label()",
                    Arrays.asList(
                        Pair.of(createBindings("x", "software"), Arrays.asList("software")),
                        Pair.of(createBindings("x", "person"), Arrays.asList("person"))
                    ),
                    TinkerGraph.open()
                },
                {
                    "g.addV().property(T.id, x).id()",
                    Arrays.asList(
                        Pair.of(createBindings("x", "abc"), Arrays.asList("abc")),
                        Pair.of(createBindings("x", "xyz"), Arrays.asList("xyz"))
                    ),
                    TinkerGraph.open(),
                },
        });
    }

    private static Bindings createBindings(final Object... pairs) {
        final Bindings bindings = new SimpleBindings();
        final Map<String,Object> args = CollectionUtil.asMap(pairs);
        args.forEach(bindings::put);
        return bindings;
    }

    @Before
    public void setup() {
        // create a new engine each time to keep the cache clear
        scriptEngine = new GremlinLangScriptEngine(variableResolverCustomizer, gremlinLangCustomizer);
        g = traversal().with(graph);
    }

    @Test
    public void shouldUseCacheForRepeatedScriptsWithVars() throws ScriptException {
        // store all traversal results to verify they are different instances
        final List<Object> results = Arrays.asList(new Object[bindingsAndResults.size()]);

        // execute the script with each set of bindings and store the results
        for (int i = 0; i < bindingsAndResults.size(); i++) {
            final Pair<Bindings, List<Object>> pair = bindingsAndResults.get(i);
            final Bindings bindings = pair.getLeft();
            final List<Object> expectedResults = pair.getRight();
            bindings.put("g", g);

            // execute the script
            final Object result = scriptEngine.eval(gremlinScript, bindings);
            assertThat(result, instanceOf(Traversal.Admin.class));

            // store the result for later comparison
            results.set(i, result);

            // verify the result matches the expected value
            for (Object expected : expectedResults) {
                assertEquals(expected, ((Traversal) result).next());
            }
        }

        // verify that each traversal instance is unique
        for (int i = 0; i < results.size(); i++) {
            for (int j = i + 1; j < results.size(); j++) {
                assertThat(results.get(i) != results.get(j), is(true));
            }
        }
    }
}
