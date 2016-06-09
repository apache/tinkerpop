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

package org.apache.tinkerpop.gremlin.python;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.VariantGraphProvider;
import org.apache.tinkerpop.gremlin.python.GremlinPythonGenerator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonProvider extends VariantGraphProvider {

    private static final Random RANDOM = new Random();

    static {
        try {
            final String rootPackageName = (new File("gremlin-variant").exists() ? "gremlin-variant/" : "") + "src/main/jython/";
            final String gremlinPythonPackageName = rootPackageName + "/gremlin_python";
            final String gremlinDriverPackageName = rootPackageName + "/gremlin_driver";
            final String gremlinPythonModuleName = gremlinPythonPackageName + "/gremlin_python.py";
            GremlinPythonGenerator.create(gremlinPythonModuleName);
            final ScriptEngine jythonEngine = ScriptEngineCache.get("jython");
            jythonEngine.eval("import sys");
            jythonEngine.eval("sys.path.append('" + gremlinPythonPackageName + "')");
            jythonEngine.eval("sys.path.append('" + gremlinDriverPackageName + "')");
            jythonEngine.eval("from gremlin_python import *");
            jythonEngine.eval("from gremlin_python import __");
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        final boolean importStatics = RANDOM.nextBoolean();
        try {
            if (importStatics)
                ScriptEngineCache.get("jython").eval("for k in statics:\n  globals()[k] = statics[k]");
        } catch (final ScriptException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        //
        if ((Boolean) graph.configuration().getProperty("skipTest"))
            return graph.traversal();
            //throw new VerificationException("This test current does not work with Gremlin-Python", EmptyTraversal.instance());
        else
            return graph.traversal(PythonTranslator.of("gremlin-groovy", "g", importStatics));
    }

}
