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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.util.Gremlin;

import javax.script.ScriptEngine;
import java.util.Collections;
import java.util.List;

/**
 * A simple base implementation of the {@link GremlinScriptEngineFactory}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinScriptEngineFactory implements GremlinScriptEngineFactory {

    private final String engineName;
    private final String languageName;
    private final List<String> extensions;
    private final List<String> mimeTypes;

    protected GremlinScriptEngineManager manager;

    public AbstractGremlinScriptEngineFactory(final String engineName, final String languageName,
                                              final List<String> extensions, final List<String> mimeTypes) {
        this.engineName = engineName;
        this.languageName = languageName;
        this.extensions = Collections.unmodifiableList(extensions);
        this.mimeTypes = Collections.unmodifiableList(mimeTypes);
    }

    @Override
    public void setCustomizerManager(final GremlinScriptEngineManager manager) {
        this.manager = manager;
    }

    @Override
    public String getEngineName() {
        return engineName;
    }

    @Override
    public String getEngineVersion() {
        return Gremlin.version();
    }

    @Override
    public List<String> getExtensions() {
        return extensions;
    }

    @Override
    public String getLanguageName() {
        return languageName;
    }

    @Override
    public String getLanguageVersion() {
        return Gremlin.version();
    }

    @Override
    public List<String> getMimeTypes() {
        return mimeTypes;
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList(languageName);
    }

    @Override
    public Object getParameter(final String key) {
        if (key.equals(ScriptEngine.ENGINE)) {
            return this.getEngineName();
        } else if (key.equals(ScriptEngine.ENGINE_VERSION)) {
            return this.getEngineVersion();
        } else if (key.equals(ScriptEngine.NAME)) {
            return engineName;
        } else if (key.equals(ScriptEngine.LANGUAGE)) {
            return this.getLanguageName();
        } else if (key.equals(ScriptEngine.LANGUAGE_VERSION)) {
            return this.getLanguageVersion();
        } else
            return null;
    }

    /**
     * Statements are concatenated together by a line feed.
     */
    @Override
    public String getProgram(final String... statements) {
        final StringBuilder program = new StringBuilder();

        for (String statement : statements) {
            program.append(statement).append(System.lineSeparator());
        }

        return program.toString();
    }
}
