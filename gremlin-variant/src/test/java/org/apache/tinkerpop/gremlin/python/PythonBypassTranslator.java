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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.Translator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.ScriptEngineCache;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PythonBypassTranslator extends PythonTranslator {

    private PythonBypassTranslator(final String alias, final boolean importStatics) {
        super(alias, importStatics);
    }

    public static PythonBypassTranslator of(final String alias) {
        return new PythonBypassTranslator(alias, false);
    }

    public static PythonBypassTranslator of(final String alias, final boolean importStatics) {
        return new PythonBypassTranslator(alias, importStatics);
    }

    @Override
    public void addStep(final Traversal.Admin<?, ?> traversal, final String stepName, final Object... arguments) {
        super.addStep(traversal, stepName, arguments);
        if (!this.importStatics)
            assert this.traversalScript.toString().startsWith(this.alias + ".");
    }

    @Override
    public Translator getAnonymousTraversalTranslator() {
        return new PythonBypassTranslator("__", this.importStatics);
    }

    @Override
    public String getExecutionLanguage() {
        return "gremlin-groovy";
    }

    @Override
    public String getTraversalScript() {
        final String traversal = super.getTraversalScript();
        if (!this.alias.equals("__")) {
            try {
                final ScriptEngine jythonEngine = ScriptEngineCache.get("jython");
                final Bindings jythonBindings = new SimpleBindings();
                jythonBindings.put(this.alias, jythonEngine.eval("PythonGraphTraversalSource(GroovyTranslator(\"" + this.alias + "\"))"));
                jythonEngine.getContext().setBindings(jythonBindings, ScriptContext.GLOBAL_SCOPE);
                return jythonEngine.eval(traversal).toString();
            } catch (final ScriptException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        } else
            return traversal;
    }

    @Override
    public String toString() {
        return StringFactory.translatorString(this);
    }
}
