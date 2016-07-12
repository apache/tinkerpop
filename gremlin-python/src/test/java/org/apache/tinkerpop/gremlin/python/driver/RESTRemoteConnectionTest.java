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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RESTRemoteConnectionTest {

    private static final ScriptEngine jython = ScriptEngineCache.get("jython");

    private final List<String> aliases = Arrays.asList("g", "j");

    @BeforeClass
    public static void setup() {
        try {
            JythonScriptEngineSetup.setup();
            jython.getContext().getBindings(ScriptContext.ENGINE_SCOPE)
                    .put("g", jython.eval("RemoteGraph(GroovyTranslator('g'), RESTRemoteConnection('http://localhost:8182')).traversal()"));
            jython.getContext().getBindings(ScriptContext.ENGINE_SCOPE)
                    .put("j", jython.eval("RemoteGraph(JythonTranslator('g'), RESTRemoteConnection('http://localhost:8182')).traversal()"));
            new GremlinServer(Settings.read(RESTRemoteConnectionTest.class.getResourceAsStream("gremlin-server-rest-modern.yaml"))).start().join();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testGraphTraversalNext() throws Exception {
        for (final String alias : this.aliases) {
            final String result = (String) jython.eval(alias + ".V().repeat(__.out()).times(2).name.next()");
            assertTrue(result.equals("lop") || result.equals("ripple"));
        }
    }

    @Test
    public void testGraphTraversalToList() throws Exception {
        for (final String alias : this.aliases) {
            final List<String> results = (List) jython.eval(alias + ".V().repeat(__.out()).times(2).name.toList()");
            assertEquals(2, results.size());
            assertTrue(results.contains("lop"));
            assertTrue(results.contains("ripple"));
        }
    }

    @Test
    public void testGraphTraversalToSet() throws Exception {
        for (final String alias : this.aliases) {
            final Set<String> results = (Set) jython.eval(alias + ".V().repeat(__.both()).times(4).hasLabel('software').name.toSet()");
            assertEquals(2, results.size());
            assertTrue(results.contains("lop"));
            assertTrue(results.contains("ripple"));
        }
    }

    @Test
    public void testGraphTraversalNextAmount() throws Exception {
        for (final String alias : this.aliases) {
            List<String> results = (List) jython.eval(alias + ".V().repeat(__.out()).times(2).name.next(2)");
            assertEquals(2, results.size());
            assertTrue(results.contains("lop"));
            assertTrue(results.contains("ripple"));
            //
            results = (List) jython.eval(alias + ".V().repeat(__.out()).times(2).name.next(4)");
            assertEquals(2, results.size());
            assertTrue(results.contains("lop"));
            assertTrue(results.contains("ripple"));
        }
    }

    @Test
    public void testRemoteConnectionBindings() throws Exception {
        for (final String alias : this.aliases) {
            final String traversalScript = jython.eval(alias + ".V().out(('a','knows'),'created')").toString();
            assertEquals(traversalScript, "g.V().out(a, \"created\")"); // ensure the traversal string is binding based
            final List<String> results = (List) jython.eval(alias + ".V().out(('a','knows')).out('created').name.next(2)");
            assertEquals(2, results.size());
            assertTrue(results.contains("lop"));
            assertTrue(results.contains("ripple"));

        }
    }
}
