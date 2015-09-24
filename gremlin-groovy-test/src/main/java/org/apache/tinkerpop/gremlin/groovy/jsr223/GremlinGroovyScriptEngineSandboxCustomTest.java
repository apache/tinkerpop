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
public class GremlinGroovyScriptEngineSandboxCustomTest extends AbstractGremlinTest {
    @Test
    public void shouldEvalGVariableAsSomethingOtherThanGraphTraversalSource() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(SandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", 1);
            engine.eval("g+1", bindings);
            fail("Should have a compile error because 'g' is expected to be GraphTraversalSource");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Cannot find matching method"));
        }

        final CompilerCustomizerProvider customSandbox = new CompileStaticCustomizerProvider(
                RebindAllVariableTypesSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", 1);
            assertEquals(2, engine.eval("g+1", bindings));
        }
    }

    @Test
    public void shouldEvalGraphVariableAsSomethingOtherThanGraph() throws Exception {
        final CompilerCustomizerProvider standardSandbox = new CompileStaticCustomizerProvider(SandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(standardSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", 1);
            engine.eval("graph+1", bindings);
            fail("Should have a compile error because 'graph' is expected to be Graph");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getCause().getMessage(), containsString("Cannot find matching method"));
        }

        final CompilerCustomizerProvider customSandbox = new CompileStaticCustomizerProvider(
                RebindAllVariableTypesSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", 1);
            assertEquals(2, engine.eval("graph+1", bindings));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotEvalBecauseSandboxIsConfiguredToNotAcceptGraphInstances() throws Exception {
        final CompilerCustomizerProvider customSandbox = new CompileStaticCustomizerProvider(
                BlockSomeVariablesSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", graph);
            engine.eval("graph.vertices()", bindings);
            fail("Should have a compile error because sandbox does not allow Graph");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [graph] is undeclared."));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotEvalBecauseSandboxIsConfiguredToNotAcceptShortVarNames() throws Exception {
        final CompilerCustomizerProvider customSandbox = new CompileStaticCustomizerProvider(
                BlockSomeVariablesSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("g", 1);
            engine.eval("g+1", bindings);
            fail("Should have a compile error because sandbox wants names > 3");
        } catch (Exception ex) {
            assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
            assertThat(ex.getMessage(), containsString("The variable [g] is undeclared."));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvalAsVariableRequirementsAreInRangeOfSandbox() throws Exception {
        final CompilerCustomizerProvider customSandbox = new CompileStaticCustomizerProvider(
                BlockSomeVariablesSandboxExtension.class.getName());
        try (GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(customSandbox)) {
            final Bindings bindings = engine.createBindings();
            bindings.put("graph", 1);
            assertEquals(2, engine.eval("graph+1", bindings));
        }
    }
}
