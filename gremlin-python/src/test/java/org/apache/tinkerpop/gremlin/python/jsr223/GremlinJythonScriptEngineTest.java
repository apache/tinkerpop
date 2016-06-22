/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinJythonScriptEngineTest {

    @Test
    public void shouldGetEngineByName() throws Exception {
        final ScriptEngine engine = new ScriptEngineManager().getEngineByName("gremlin-jython");
        assertNotNull(engine);
        assertTrue(engine instanceof GremlinJythonScriptEngine);
        assertTrue(engine.eval("Graph") instanceof Class);
        assertEquals(3, engine.eval("1+2"));
    }

    @Test
    public void shouldHaveStandardImports() throws Exception {
        final ScriptEngine engine = new ScriptEngineManager().getEngineByName("gremlin-jython");
        assertTrue(engine.eval("Graph") instanceof Class);
    }
}
