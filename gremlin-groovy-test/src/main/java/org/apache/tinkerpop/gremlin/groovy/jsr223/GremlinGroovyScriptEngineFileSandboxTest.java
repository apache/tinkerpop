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
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.FileSandboxExtension;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.script.Bindings;

import java.io.File;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineFileSandboxTest extends AbstractGremlinTest {
    @BeforeClass
    public static void init() throws Exception {
        final File f = TestHelper.generateTempFileFromResource(GremlinGroovyScriptEngineFileSandboxTest.class, "sandbox.yaml", ".yaml");
        System.setProperty(FileSandboxExtension.GREMLIN_SERVER_SANDBOX, f.getAbsolutePath());
    }

    @AfterClass
    public static void destroy() {
        System.clearProperty(FileSandboxExtension.GREMLIN_SERVER_SANDBOX);
    }

    @Test
    public void shouldEvalAsTheMethodIsWhiteListed() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(FileSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            assertEquals(123, engine.eval("java.lang.Math.abs(-123)"));
            assertThat(engine.eval("new Boolean(true)"), is(true));
            assertThat(engine.eval("new Boolean(true).toString()"), is("true"));
        }
    }

    @Test
    public void shouldEvalAsGroovyPropertiesWhenWhiteListed() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(FileSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            assertThat(Arrays.equals("test".getBytes(), (byte[]) engine.eval("'test'.bytes")), is(true));
        }
    }

    @Test
    public void shouldPreventMaliciousStuffWithSystemButAllowSomeMethodsOnSystem() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(FileSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            assertThat((long) engine.eval("System.currentTimeMillis()"), greaterThan(0l));
            assertThat((long) engine.eval("System.nanoTime()"), greaterThan(0l));

            engine.eval("System.exit(0)");
            fail("Should have a compile error because class/method is not white listed");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Not authorized to call this method"));
        }
    }

    @Test
    public void shouldPreventMaliciousStuffWithFile() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(FileSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            engine.eval("java.nio.file.FileSystems.getDefault()");
            fail("Should have a compile error because class/method is not white listed");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Not authorized to call this method"));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalOnGAsTheMethodIsWhiteListed() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(FileSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", g);
            bindings.put("marko", convertToVertexId("marko"));
            assertEquals(g.V(convertToVertexId("marko")).next(), engine.eval("g.V(marko).next()", bindings));
            assertEquals(g.V(convertToVertexId("marko")).out("created").count().next(), engine.eval("g.V(marko).out(\"created\").count().next()", bindings));
        }
    }
}
