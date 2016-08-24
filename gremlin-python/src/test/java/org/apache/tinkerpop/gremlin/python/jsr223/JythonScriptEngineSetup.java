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

import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JythonScriptEngineSetup {

    private JythonScriptEngineSetup() {
    }

    public static void setup() {
        try {
            final ScriptEngine jythonEngine = ScriptEngineCache.get("jython");
            if (null != System.getenv("PYTHONPATH")) {
                jythonEngine.eval("import sys");
                jythonEngine.eval("sys.path.append('" + System.getenv("PYTHONPATH") + "')");
            }
            jythonEngine.eval("import gremlin_python.statics");
            jythonEngine.eval("from gremlin_python.process.traversal import *");
            jythonEngine.eval("from gremlin_python.process.graph_traversal import *");
            jythonEngine.eval("from gremlin_python.process.graph_traversal import __");
            // jythonEngine.eval("from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection");
            jythonEngine.eval("from gremlin_python.process.traversal import Bytecode");
            jythonEngine.eval("from gremlin_python.structure.graph import Graph");
            jythonEngine.eval("from gremlin_python.structure.io.graphson import GraphSONWriter");
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
