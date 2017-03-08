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

import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompilationOptionsCustomizerProvider;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineCompilationOptionsTest {
    @Test
    public void shouldRegisterLongCompilationDeprecated() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new CompilationOptionsCustomizerProvider(10));

        final int numberOfParameters = 3000;
        final Bindings b = new SimpleBindings();

        // generate a script that takes a long time to compile
        String script = "x = 0";
        for (int ix = 0; ix < numberOfParameters; ix++) {
            if (ix > 0 && ix % 100 == 0) {
                script = script + ";" + System.lineSeparator() + "x = x";
            }
            script = script + " + x" + ix;
            b.put("x" + ix, ix);
        }

        assertEquals(0, engine.getLongRunCompilationCount());

        engine.eval(script, b);

        assertEquals(1, engine.getLongRunCompilationCount());
    }

    @Test
    public void shouldRegisterLongCompilation() throws Exception {
        final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine(new CompilationOptionsCustomizer(10));

        final int numberOfParameters = 3000;
        final Bindings b = new SimpleBindings();

        // generate a script that takes a long time to compile
        String script = "x = 0";
        for (int ix = 0; ix < numberOfParameters; ix++) {
            if (ix > 0 && ix % 100 == 0) {
                script = script + ";" + System.lineSeparator() + "x = x";
            }
            script = script + " + x" + ix;
            b.put("x" + ix, ix);
        }

        assertEquals(0, engine.getLongRunCompilationCount());

        engine.eval(script, b);

        assertEquals(1, engine.getLongRunCompilationCount());
    }
}
