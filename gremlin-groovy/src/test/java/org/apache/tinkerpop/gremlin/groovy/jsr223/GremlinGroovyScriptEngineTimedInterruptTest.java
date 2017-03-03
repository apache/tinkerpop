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

import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineTimedInterruptTest {

    @Test
    public void shouldTimeoutScriptOnTimedWhile() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new TimedInterruptGroovyCustomizer(1000));
        try {
            engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            fail("This should have timed out");
        } catch (ScriptException se) {
            assertEquals(org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException.class, se.getCause().getCause().getClass());
        }
    }

    @Test
    public void shouldTimeoutScriptOnTimedWhileOnceEngineHasBeenAliveForLongerThanTimeout() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new TimedInterruptGroovyCustomizer(1000));
        Thread.sleep(2000);
        try {
            engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 10000) {}");
            fail("This should have timed out");
        } catch (ScriptException se) {
            assertEquals(org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException.class, se.getCause().getCause().getClass());
        }

        assertEquals(2, engine.eval("1+1"));
    }

    @Test
    public void shouldContinueToEvalScriptsEvenWithTimedInterrupt() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new TimedInterruptGroovyCustomizer(1000));

        for (int ix = 0; ix < 5; ix++) {
            try {
                // this script takes 1000 ms longer than the interruptionTimeout
                engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 2000) {}");
                fail("This should have timed out");
            } catch (ScriptException se) {
                assertEquals(org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException.class, se.getCause().getCause().getClass());
            }

            // this script takes 500 ms less than the interruptionTimeout
            assertEquals("test", engine.eval("s = System.currentTimeMillis();\nwhile((System.currentTimeMillis() - s) < 500) {};'test'"));
        }

        assertEquals(2, engine.eval("1+1"));
    }

    @Test
    public void shouldNotTimeoutStandaloneFunction() throws Exception {
        // use a super fast timeout which should not prevent the call of a cached function
        final ScriptEngine engine = new GremlinGroovyScriptEngine(new TimedInterruptGroovyCustomizer(1));
        engine.eval("def addItUp(x,y) { x + y }");

        assertEquals(3, engine.eval("addItUp(1,2)"));
    }
}
