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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite.ENGINE_TO_TEST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.CombinableMatcher.either;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * This is an important test case in that it validates that core features of {@code ScriptEngine} instances that claim
 * to be "Gremlin-enabled" work in the expected fashion.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinEnabledScriptEngineTest {
    private static final GremlinScriptEngineManager manager = new DefaultGremlinScriptEngineManager();

    @Test
    public void shouldEvalBytecode() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();

        // purposefully use "x" to match the name of the traversal source binding for "x" below and
        // thus tests the alias added for "x"
        final GraphTraversal t = getTraversalWithLambda(g);

        final Bindings bindings = new SimpleBindings();
        bindings.put("x", g);

        final Traversal evald = scriptEngine.eval(t.asAdmin().getBytecode(), bindings, "x");

        assertTraversals(t, evald);

        assertThat(manager.getBindings().containsKey(GremlinScriptEngine.HIDDEN_G), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBytecodeEvalWithAliasInBindings() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();

        // purposefully use "x" to match the name of the traversal source binding for "x" below and
        // thus tests the alias added for "x"
        final GraphTraversal t = getTraversalWithLambda(g);

        final Bindings bindings = new SimpleBindings();
        bindings.put("x", g);
        bindings.put(GremlinScriptEngine.HIDDEN_G, g);

        scriptEngine.eval(t.asAdmin().getBytecode(), bindings, "x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBytecodeEvalWithAliasAsTraversalSource() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();

        // purposefully use "x" to match the name of the traversal source binding for "x" below and
        // thus tests the alias added for "x"
        final GraphTraversal t = getTraversalWithLambda(g);

        final Bindings bindings = new SimpleBindings();
        bindings.put("x", g);

        scriptEngine.eval(t.asAdmin().getBytecode(), bindings, GremlinScriptEngine.HIDDEN_G);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBytecodeEvalWithMissingBinding() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();

        // purposefully use "x" to match the name of the traversal source binding for "x" below and
        // thus tests the alias added for "x"
        final GraphTraversal t = getTraversalWithLambda(g);

        final Bindings bindings = new SimpleBindings();
        bindings.put("z", g);

        scriptEngine.eval(t.asAdmin().getBytecode(), bindings, "x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowBytecodeEvalWithInvalidBinding() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal();

        // purposefully use "x" to match the name of the traversal source binding for "x" below and
        // thus tests the alias added for "x"
        final GraphTraversal t = getTraversalWithLambda(g);

        final Bindings bindings = new SimpleBindings();
        bindings.put("z", g);
        bindings.put("x", "invalid-binding-for-x-given-x-should-be-traversal-source");

        scriptEngine.eval(t.asAdmin().getBytecode(), bindings, "x");
    }

    @Test
    public void shouldGetEngineByName() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        assertEquals(ENGINE_TO_TEST, scriptEngine.getFactory().getEngineName());
    }

    @Test
    public void shouldHaveCoreImportsInPlace() throws Exception {
        final GremlinScriptEngine scriptEngine = manager.getEngineByName(ENGINE_TO_TEST);
        final List<Class> classesToCheck = Arrays.asList(Vertex.class, Edge.class, Graph.class, VertexProperty.class);
        for (Class clazz : classesToCheck) {
            assertEquals(clazz, scriptEngine.eval(clazz.getSimpleName()));
        }
    }

    @Test
    public void shouldReturnNoCustomizers() {
        final GremlinScriptEngineManager mgr = new DefaultGremlinScriptEngineManager();
        mgr.addPlugin(ImportGremlinPlugin.build()
                .classImports(java.awt.Color.class)
                .appliesTo(Collections.singletonList("fake-script-engine")).create());
        assertEquals(0, mgr.getCustomizers(ENGINE_TO_TEST).size());
    }

    private static GraphTraversal<Vertex, Long> getTraversalWithLambda(final GraphTraversalSource g) {
        assumeThat("This test is not enabled for this ScriptEngine: " + ENGINE_TO_TEST, ENGINE_TO_TEST,
                anyOf(is("gremlin-python"), is("gremlin-jython"), is("gremlin-groovy")));
        if (ENGINE_TO_TEST.equals("gremlin-groovy"))
            return g.V().out("created").map(Lambda.function("{x -> x.get().values('name')}")).count();
        else if (ENGINE_TO_TEST.equals("gremlin-python") || ENGINE_TO_TEST.equals("gremlin-jython"))
            return g.V().out("created").map(Lambda.function("x : x.get().values('name')")).count();
        else
            throw new RuntimeException("The " + ENGINE_TO_TEST + " ScriptEngine is not supported by this test");
    }

    private static void assertTraversals(final GraphTraversal t, final Traversal evald) {
        final List<Step> steps = t.asAdmin().getSteps();
        for (int ix = 0; ix < steps.size(); ix++) {
            assertEquals(steps.get(ix).getClass(), evald.asAdmin().getSteps().get(ix).getClass());
        }
    }
}
