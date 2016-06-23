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

package org.apache.tinkerpop.gremlin.java.translator.jython;

import org.apache.tinkerpop.gremlin.java.translator.PythonTranslatorProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonJythonTranslatorProvider extends PythonTranslatorProvider {

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if ((Boolean) graph.configuration().getProperty("skipTest"))
            return graph.traversal();
            //throw new VerificationException("This test current does not work with Gremlin-Python", EmptyTraversal.instance());
        else {
            try {
                if (IMPORT_STATICS)
                    ScriptEngineCache.get("jython").eval("for k in statics:\n  globals()[k] = statics[k]");
                else
                    ScriptEngineCache.get("jython").eval("for k in statics:\n  if k in globals():\n    del globals()[k]");
            } catch (final ScriptException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            return graph.traversal().withTranslator(PythonJythonTranslator.of("g", IMPORT_STATICS)); // the bypass translator will ensure that gremlin-groovy is ultimately used
        }
    }

}
