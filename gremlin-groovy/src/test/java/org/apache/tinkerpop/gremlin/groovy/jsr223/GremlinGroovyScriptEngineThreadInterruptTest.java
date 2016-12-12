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

import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.ThreadInterruptCustomizerProvider;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineThreadInterruptTest {
    @Test
    public void shouldInterruptWhileDeprecated() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new ThreadInterruptCustomizerProvider());
        final AtomicBoolean asserted = new AtomicBoolean(false);

        final Thread t = new Thread(() -> {
            try {
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            } catch (ScriptException se) {
                asserted.set(se.getCause().getCause() instanceof InterruptedException);
            }
        });

        t.start();
        Thread.sleep(100);
        t.interrupt();
        while(t.isAlive()) {}

        assertTrue(asserted.get());
    }

    @Test
    public void shouldInterruptWhile() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new ThreadInterruptGroovyCustomizer());
        final AtomicBoolean asserted = new AtomicBoolean(false);

        final Thread t = new Thread(() -> {
            try {
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            } catch (ScriptException se) {
                asserted.set(se.getCause().getCause() instanceof InterruptedException);
            }
        });

        t.start();
        Thread.sleep(100);
        t.interrupt();
        while(t.isAlive()) {}

        assertTrue(asserted.get());
    }

    @Test
    public void shouldNotInterruptWhile() throws Exception {
        // companion to shouldInterruptWhile                                 t
        final ScriptEngine engine = new GremlinGroovyScriptEngine();
        final AtomicBoolean exceptionFired = new AtomicBoolean(true);

        final Thread t = new Thread(() -> {
            try {
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 2000) {};'test'");
                exceptionFired.set(false);
            } catch (ScriptException se) {
                se.printStackTrace();
            }
        });

        t.start();
        Thread.sleep(100);
        t.interrupt();
        while(t.isAlive()) {}

        assertFalse(exceptionFired.get());
    }
}
