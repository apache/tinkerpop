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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SimpleSandboxExtension;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TypeCheckedCustomizerProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.script.Bindings;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GremlinGroovyScriptEngineSandboxedStandardTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TypeCheckedCustomizerProvider.class.getSimpleName(), new TypeCheckedCustomizerProvider(), new TypeCheckedCustomizerProvider(SimpleSandboxExtension.class.getName())},
                {CompileStaticCustomizerProvider.class.getSimpleName(), new CompileStaticCustomizerProvider(), new CompileStaticCustomizerProvider(SimpleSandboxExtension.class.getName())}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public CompilerCustomizerProvider notSandboxed;

    @Parameterized.Parameter(value = 2)
    public CompilerCustomizerProvider sandboxed;

    @Test
    public void shouldEvalGraphTraversalSource() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            assertEquals(g.V(convertToVertexId(graph, "marko")).next(), engine.eval("g.V(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            engine.eval("g.V(marko).next()", bindings);
            fail("Type checking should have forced an error as 'g' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [g] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            assertEquals(g.V(convertToVertexId(graph, "marko")).next(), engine.eval("g.V(marko).next()", bindings));
            assertEquals(g.V(convertToVertexId(graph, "marko")).out("created").count().next(), engine.eval("g.V(marko).out(\"created\").count().next()", bindings));
        }
    }

    @Test
    public void shouldEvalGraph() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            assertEquals(graph.vertices(convertToVertexId(graph, "marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            assertEquals(graph.vertices(convertToVertexId(graph, "marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
            fail("Type checking should have forced an error as 'graph' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [graph] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("x", convertToVertexId(graph, "marko"));
            assertEquals(graph.vertices(convertToVertexId(graph, "marko")).next(), engine.eval("graph.vertices(x).next()", bindings));
            fail("Type checking should have forced an error as 'graph' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [graph] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId(graph, "marko"));
            assertEquals(graph.vertices(convertToVertexId(graph, "marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("x", convertToVertexId(graph, "marko"));
            assertEquals(graph.vertices(convertToVertexId(graph, "marko")).next(), engine.eval("graph.vertices(x).next()", bindings));
        }
    }

    private Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    private Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graph.traversal().V().has("name", vertexName).next();
    }
}
