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

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SandboxExtension;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TypeCheckedCustomizerProvider;
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
public class GremlinGroovyScriptEngineSandboxedStandardTest extends AbstractGremlinTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TypeCheckedCustomizerProvider.class.getSimpleName(), new TypeCheckedCustomizerProvider(), new TypeCheckedCustomizerProvider(SandboxExtension.class.getName())},
                {CompileStaticCustomizerProvider.class.getSimpleName(), new CompileStaticCustomizerProvider(), new CompileStaticCustomizerProvider(SandboxExtension.class.getName())}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public CompilerCustomizerProvider notSandboxed;

    @Parameterized.Parameter(value = 2)
    public CompilerCustomizerProvider sandboxed;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalGraphTraversalSource() throws Exception {
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId("marko"));
            engine.eval("g.V(marko).next()", bindings);
            fail("Type checking should have forced an error as 'g' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [g] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()", bindings));
            assertEquals(g.V(convertToVertexId("marko")).out("created").count().next(), engine.eval("g.V(marko).out(\"created\").count().next()", bindings));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalGraph() throws Exception {
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine()) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(graph.vertices(convertToVertexId("marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(graph.vertices(convertToVertexId("marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
            fail("Type checking should have forced an error as 'graph' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [graph] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(notSandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("x", convertToVertexId("marko"));
            assertEquals(graph.vertices(convertToVertexId("marko")).next(), engine.eval("graph.vertices(x).next()", bindings));
            fail("Type checking should have forced an error as 'graph' is not defined");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [graph] is undeclared."));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(graph.vertices(convertToVertexId("marko")).next(), engine.eval("graph.vertices(marko).next()", bindings));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(sandboxed)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            bindings.put("x", convertToVertexId("marko"));
            assertEquals(graph.vertices(convertToVertexId("marko")).next(), engine.eval("graph.vertices(x).next()", bindings));
        }
    }
}
