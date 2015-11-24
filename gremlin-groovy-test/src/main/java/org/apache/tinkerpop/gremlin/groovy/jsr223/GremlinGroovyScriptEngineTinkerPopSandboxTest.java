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
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TinkerPopSandboxExtension;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.junit.Test;

import javax.script.Bindings;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTinkerPopSandboxTest extends AbstractGremlinTest {
    @Test
    public void shouldNotEvalAsTheMethodIsNotWhiteListed() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(TinkerPopSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            engine.eval("java.lang.Math.abs(123)");
            fail("Should have a compile error because class/method is not white listed");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Not authorized to call this method"));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalOnGAsTheMethodIsWhiteListed() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(TinkerPopSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()", bindings));
            assertEquals(g.V(convertToVertexId("marko")).out("created").count().next(), engine.eval("g.V(marko).out(\"created\").count().next()", bindings));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotEvalColorWhenCallingMethods() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(AllowColorSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            assertEquals(new java.awt.Color(255,255,255), engine.eval("new java.awt.Color(255,255,255)"));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            engine.eval("new java.awt.Color(255,255,255).getRed()");
            fail("Type checking should have forced an error as 'getRed()' is not authorized - just Color construction");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("Not authorized to call this method"));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotEvalColorWhenCallingProperties() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(AllowColorSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            assertEquals(new java.awt.Color(255,255,255), engine.eval("new java.awt.Color(255,255,255)"));
        }

        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            engine.eval("new java.awt.Color(255,255,255).red");
            fail("Type checking should have forced an error as 'red' is not authorized - just Color construction");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("Not authorized to call this method"));
        }
    }
}
