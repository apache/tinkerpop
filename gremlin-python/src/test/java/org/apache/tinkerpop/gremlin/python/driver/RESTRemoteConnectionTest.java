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

package org.apache.tinkerpop.gremlin.python.driver;

import org.apache.tinkerpop.gremlin.python.jsr223.JythonScriptEngineSetup;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RESTRemoteConnectionTest {

    private static final ScriptEngine jython = ScriptEngineCache.get("jython");

    @BeforeClass
    public static void setup() {
        try {
            JythonScriptEngineSetup.setup();
            jython.getContext().getBindings(ScriptContext.ENGINE_SCOPE)
                    .put("g", jython.eval("PythonGraphTraversalSource(GroovyTranslator('g'), RESTRemoteConnection('http://localhost:8182'))"));
            jython.getContext().getBindings(ScriptContext.ENGINE_SCOPE)
                    .put("h", jython.eval("PythonGraphTraversalSource(JythonTranslator('g'), RESTRemoteConnection('http://localhost:8182'))"));
            final GremlinServer server = new GremlinServer(Settings.read(RESTRemoteConnectionTest.class.getResourceAsStream("gremlin-server-rest-modern.yaml")));
            server.start().join();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testPythonGraphTraversalNext() throws Exception {
        String result = (String) jython.eval("g.V().repeat(__.out()).times(2).name.next()");
        assertTrue(result.equals("lop") || result.equals("ripple"));
        result = (String) jython.eval("h.V().repeat(__.out()).times(2).name.next()");
        assertTrue(result.equals("lop") || result.equals("ripple"));
    }

    @Test
    public void testPythonGraphTraversalToList() throws Exception {
        List<String> results = (List) jython.eval("g.V().repeat(__.out()).times(2).name.toList()");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
        //
        results = (List) jython.eval("h.V().repeat(__.out()).times(2).name.toList()");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
    }

    @Test
    public void testPythonGraphTraversalToSet() throws Exception {
        Set<String> results = (Set) jython.eval("g.V().repeat(__.both()).times(4).hasLabel('software').name.toSet()");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
        //
        results = (Set) jython.eval("h.V().repeat(__.both()).times(4).hasLabel('software').name.toSet()");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
    }

    @Test
    public void testPythonGraphTraversalNextAmount() throws Exception {
        List<String> results = (List) jython.eval("g.V().repeat(__.out()).times(2).name.next(2)");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
        //
        results = (List) jython.eval("g.V().repeat(__.out()).times(2).name.next(4)");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));

        ///

        results = (List) jython.eval("h.V().repeat(__.out()).times(2).name.next(2)");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
        //
        results = (List) jython.eval("h.V().repeat(__.out()).times(2).name.next(4)");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
    }
}
