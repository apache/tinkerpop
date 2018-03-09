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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.junit.Test;

import javax.script.ScriptException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTypeCheckedTest {

    @Test
    public void shouldTypeCheck() throws Exception {
        // with no type checking this should pass
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine()) {
            assertEquals(255, scriptEngine.eval("((Object) new java.awt.Color(255, 255, 255)).getRed()"));
        }

        final TypeCheckedGroovyCustomizer provider = new TypeCheckedGroovyCustomizer();
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(provider)) {
            scriptEngine.eval("((Object) new java.awt.Color(255, 255, 255)).getRed()");
            fail("Should have failed type checking");
        } catch (ScriptException se) {
            final Throwable root = ExceptionUtils.getRootCause(se);
            assertEquals(MultipleCompilationErrorsException.class, root.getClass());
            assertThat(se.getMessage(), containsString("[Static type checking] - Cannot find matching method java.lang.Object#getRed(). Please check if the declared type is correct and if the method exists."));
        }
    }

    @Test
    public void shouldTypeCheckWithExtension() throws Exception {
        // with no type checking extension this should pass
        final TypeCheckedGroovyCustomizer providerNoExtension = new TypeCheckedGroovyCustomizer();
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(providerNoExtension)) {
            assertEquals(255, scriptEngine.eval("def c = new java.awt.Color(255, 255, 255); c.red"));
        }

        final TypeCheckedGroovyCustomizer providerWithExtension = new TypeCheckedGroovyCustomizer(
                PrecompiledExtensions.PreventColorUsageExtension.class.getName());
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(providerWithExtension)) {
            scriptEngine.eval("def c = new java.awt.Color(255, 255, 255); c.red");
            fail("Should have failed type checking");
        } catch (ScriptException se) {
            assertEquals(MultipleCompilationErrorsException.class, se.getCause().getClass());
            assertThat(se.getMessage(), containsString("Method call is not allowed!"));
        }
    }

    @Test
    public void shouldTypeCheckWithMultipleExtension() throws Exception {
        // with no type checking extension this should pass
        final TypeCheckedGroovyCustomizer providerNoExtension = new TypeCheckedGroovyCustomizer();
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(providerNoExtension)) {
            assertEquals(255, scriptEngine.eval("def c = new java.awt.Color(255, 255, 255); c.red"));
            assertEquals(1l, scriptEngine.eval("def c = new java.util.concurrent.CountDownLatch(1); c.count"));
        }

        final TypeCheckedGroovyCustomizer providerWithExtension = new TypeCheckedGroovyCustomizer(
                PrecompiledExtensions.PreventColorUsageExtension.class.getName() +
                        "," + PrecompiledExtensions.PreventCountDownLatchUsageExtension.class.getName());
        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(providerWithExtension)) {
            scriptEngine.eval("def c = new java.awt.Color(255, 255, 255); c.red");
            fail("Should have failed type checking");
        } catch (ScriptException se) {
            assertEquals(MultipleCompilationErrorsException.class, se.getCause().getClass());
            assertThat(se.getMessage(), containsString("Method call is not allowed!"));
        }

        try (GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine(providerWithExtension)) {
            scriptEngine.eval("def c = new java.util.concurrent.CountDownLatch(1); c.count");
            fail("Should have failed type checking");
        } catch (ScriptException se) {
            assertEquals(MultipleCompilationErrorsException.class, se.getCause().getClass());
            assertThat(se.getMessage(), containsString("Method call is not allowed!"));
        }
    }
}
