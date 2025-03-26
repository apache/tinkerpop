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

import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.language.translator.GremlinTranslator;
import org.apache.tinkerpop.gremlin.language.translator.Translator;

import javax.script.ScriptContext;
import javax.script.ScriptException;

/**
 * Designed to be used in tests.
 * Converts incoming script from gremlin-lang to gremlin-groovy compatible form and executes with
 * {@code GremlinGroovyScriptEngine}.
 */

public class GremlinGroovyScriptTestEngine extends GremlinGroovyScriptEngine {

    public GremlinGroovyScriptTestEngine() {

    }

    @Override
    public GremlinScriptEngineFactory getFactory() {
        return new GremlinGroovyScriptTestEngineFactory();
    }

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        final String translatedScript = GremlinTranslator.translate(script, Translator.GROOVY).getTranslated();
        return super.eval(translatedScript, context);
    }
}
