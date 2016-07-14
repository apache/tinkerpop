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

import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.ScriptEngine;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinJythonScriptEngineFactory extends PyScriptEngineFactory implements GremlinScriptEngineFactory {

    private static final String ENGINE_NAME = "gremlin-jython";
    private static final String LANGUAGE_NAME = "gremlin-jython";
    private static final String PLAIN = "plain";
    private static final List<String> EXTENSIONS = Collections.singletonList("py");

    private GremlinScriptEngineManager manager;

    @Override
    public void setCustomizerManager(final GremlinScriptEngineManager manager) {
        this.manager = manager;
    }

    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    @Override
    public String getEngineVersion() {
        return Gremlin.version();
    }

    @Override
    public List<String> getExtensions() {
        return EXTENSIONS;
    }

    @Override
    public String getLanguageName() {
        return LANGUAGE_NAME;
    }

    @Override
    public String getLanguageVersion() {
        return Gremlin.version();
    }

    @Override
    public List<String> getMimeTypes() {
        return Collections.singletonList(PLAIN);
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList(LANGUAGE_NAME);
    }

    @Override
    public Object getParameter(final String key) {
        if (key.equals(ScriptEngine.ENGINE)) {
            return this.getEngineName();
        } else if (key.equals(ScriptEngine.ENGINE_VERSION)) {
            return this.getEngineVersion();
        } else if (key.equals(ScriptEngine.NAME)) {
            return ENGINE_NAME;
        } else if (key.equals(ScriptEngine.LANGUAGE)) {
            return this.getLanguageName();
        } else if (key.equals(ScriptEngine.LANGUAGE_VERSION)) {
            return this.getLanguageVersion();
        } else
            return super.getParameter(key);
    }

    @Override
    public GremlinScriptEngine getScriptEngine() {
        return new GremlinJythonScriptEngine();
    }
}