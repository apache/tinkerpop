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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TinkerPopSandboxExtension;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.junit.Test;

import javax.script.Bindings;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTinkerPopSandboxTest {
    @Test
    public void shouldNotEvalAsTheMethodIsNotWhiteListed() throws Exception {
        final CompileStaticGroovyCustomizer standardSandbox = new CompileStaticGroovyCustomizer(TinkerPopSandboxExtension.class.getName());
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox);
        try {
            engine.eval("java.lang.Math.abs(123)");
            fail("Should have a compile error because class/method is not white listed");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Not authorized to call this method"));
        }
    }

    @Test
    public void shouldEvalOnGAsTheMethodIsWhiteListed() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();
        final CompileStaticGroovyCustomizer standardSandbox = new CompileStaticGroovyCustomizer(TinkerPopSandboxExtension.class.getName());
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox);

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        bindings.put("marko", convertToVertexId(graph, "marko"));
        assertEquals(g.V(convertToVertexId(graph, "marko")).next(), engine.eval("g.V(marko).next()", bindings));
        assertEquals(g.V(convertToVertexId(graph, "marko")).out("created").count().next(), engine.eval("g.V(marko).out(\"created\").count().next()", bindings));
    }

    /**
     * The allow list includes {@code DefaultGroovyMethods}, so Groovy helper methods (the GDK) hosted there can be
     * called. Groovy can move GDK methods to other host classes between versions (Groovy 5 moved array methods to
     * {@code ArrayGroovyMethods}), which would silently flip these from allowed to "Not authorized".
     */
    @Test
    public void shouldAllowGroovyHelperMethodsHostedOnDefaultGroovyMethods() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(
                new CompileStaticGroovyCustomizer(TinkerPopSandboxExtension.class.getName()));
        final Bindings bindings = engine.createBindings();
        bindings.put("g", graph.traversal());

        assertEquals(Arrays.asList(2, 4, 6), engine.eval("[1, 2, 3].collect{it * 2}", bindings));
        assertEquals(6, engine.eval("[1, 2, 3].sum()", bindings));
        assertEquals(3, engine.eval("([1, 2] as int[]).sum()", bindings));
        assertEquals(Arrays.asList(1, 2), engine.eval("([1, 2] as Integer[]).collect{it}", bindings));
        assertEquals(Collections.singletonMap("a", 1), engine.eval("[a: 1].each{k, v -> k}", bindings));
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), engine.eval("g.V().toList().collect{it.id()}", bindings));
        assertEquals("marko,vadas,lop,josh,ripple,peter", engine.eval("g.V().values('name').toList().join(',')", bindings));
    }

    /**
     * The denial message names the exact method descriptor checked against the allow list, so a change in how Groovy
     * resolves a call (declaring class, parameter types) shows up here.
     */
    @Test
    public void shouldNameTheExactMethodThatIsNotAuthorized() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(
                new CompileStaticGroovyCustomizer(TinkerPopSandboxExtension.class.getName()));
        assertNotAuthorized(engine, "System.getProperty('x')", "java.lang.System#getProperty(java.lang.String)");
        assertNotAuthorized(engine, "java.lang.Math.abs(-1)", "java.lang.Math#abs(int)");
        assertNotAuthorized(engine, "new File('x').text", "java.io.File#<init>(java.lang.String)");
        // String helpers live on StringGroovyMethods, which the allow list does not include
        assertNotAuthorized(engine, "'abc'.reverse()",
                "org.codehaus.groovy.runtime.StringGroovyMethods#reverse(java.lang.CharSequence)");
        assertNotAuthorized(engine, "'a,b'.tokenize(',')",
                "org.codehaus.groovy.runtime.StringGroovyMethods#tokenize(java.lang.CharSequence,java.lang.CharSequence)");
    }

    private static void assertNotAuthorized(final GremlinGroovyScriptEngine engine, final String script,
                                            final String descriptor) {
        try {
            engine.eval(script);
            fail("Should not be authorized: " + script);
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Not authorized to call this method: " + descriptor));
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
