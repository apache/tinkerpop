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

import org.apache.tinkerpop.gremlin.java.translator.PythonTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PythonJythonTranslator extends PythonTranslator {

    private PythonJythonTranslator(final String traversalSource, final boolean importStatics) {
        super(traversalSource, "__", importStatics);
    }

    public static PythonJythonTranslator of(final String traversalSource) {
        return new PythonJythonTranslator(traversalSource, false);
    }

    public static PythonJythonTranslator of(final String traversalSource, final boolean importStatics) {
        return new PythonJythonTranslator(traversalSource, importStatics);
    }

    @Override
    public String getTargetLanguage() {
        return "gremlin-jython";
    }

    @Override
    public String translate(final Bytecode bytecode) {
        final String traversal = super.translate(bytecode);
        try {
            final ScriptEngine jythonEngine = ScriptEngineCache.get("jython");
            jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE)
                    .put(this.traversalSource, jythonEngine.eval("RemoteGraph(JythonTranslator(\"" + this.traversalSource + "\"), None).traversal()"));
            return jythonEngine.eval(traversal).toString();
        } catch (final ScriptException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }
}
