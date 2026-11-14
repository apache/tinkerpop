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
package org.apache.tinkerpop.gremlin.console

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.Customizer
import org.apache.tinkerpop.gremlin.jsr223.DefaultBindingsCustomizer
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer
import org.apache.tinkerpop.gremlin.jsr223.DefaultScriptCustomizer
import org.junit.Test

import javax.script.SimpleBindings
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

class PluggedInTest {

    // the console's engine, as the default compiler configuration cannot take import customizers
    private final GroovyEngine engine = Console.createEngine()

    private void activate(final Customizer customizer) {
        final PluggedIn pluggedIn = new PluggedIn(new AbstractGremlinPlugin("test.plugin", customizer) {}, engine, false)
        pluggedIn.activate()
        assertTrue(pluggedIn.activated)
    }

    @Test
    void shouldAddTheBindingsOfABindingsCustomizer() {
        activate(new DefaultBindingsCustomizer(new SimpleBindings([answer: 42, greeting: "hi"])))
        assertEquals(42, engine.execute("answer"))
        assertEquals("hi", engine.execute("greeting"))
    }

    @Test
    void shouldAddTheImportsOfAnImportCustomizer() {
        activate(DefaultImportCustomizer.build()
                .addClassImports(TimeUnit)
                .addMethodImports(Collections.getMethod("emptyList"))
                .addFieldImports(Math.getField("PI"))
                .create())
        assertEquals(TimeUnit.SECONDS, engine.execute("TimeUnit.SECONDS"))
        assertEquals([], engine.execute("emptyList()"))
        assertEquals(Math.PI, (double) engine.execute("PI"), 0d)
    }

    @Test
    void shouldRunTheScriptsOfAScriptCustomizer() {
        activate(new DefaultScriptCustomizer([["fromScript = 1 + 1"]] as Set<List<String>>))
        assertEquals(2, engine.execute("fromScript"))
    }
}
