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

import org.apache.tinkerpop.gremlin.VariantGraphProvider;
import org.apache.tinkerpop.gremlin.python.JythonScriptEngineSetup;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RESTRemoteConnectionTest {

    private final ScriptEngine jython = ScriptEngineCache.get("jython");

    @Before
    public void setup() {
        try {
            JythonScriptEngineSetup.setup();
            final Bindings jythonBindings = new SimpleBindings();
            jythonBindings.put("g", jython.eval("PythonGraphTraversalSource(GroovyTranslator('g'), RESTRemoteConnection('http://localhost:8182'))"));
            jython.getContext().setBindings(jythonBindings, ScriptContext.GLOBAL_SCOPE);
            final GremlinServer server = new GremlinServer(Settings.read(VariantGraphProvider.class.getResourceAsStream("gremlin-server-rest-modern.yaml")));
            server.start().join();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testRESTRemoteConnection() throws Exception {
        final List<String> results = (List) jython.eval("g.V().repeat(__.out()).times(2).name.toList()");
        assertEquals(2, results.size());
        assertTrue(results.contains("lop"));
        assertTrue(results.contains("ripple"));
    }
}
