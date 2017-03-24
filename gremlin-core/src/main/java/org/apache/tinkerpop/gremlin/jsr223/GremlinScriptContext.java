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

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.SimpleBindings;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@code ScriptContext} that doesn't create new instances of {@code Reader} and {@code Writer} classes on
 * initialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinScriptContext implements ScriptContext {

    private Writer writer;
    private Writer errorWriter;
    private Reader reader;
    private Bindings engineScope;
    private Bindings globalScope;
    private static List<Integer> scopes;

    static {
        scopes = new ArrayList<>(2);
        scopes.add(ENGINE_SCOPE);
        scopes.add(GLOBAL_SCOPE);
        scopes = Collections.unmodifiableList(scopes);
    }

    /**
     * Create a {@code GremlinScriptContext}.
     */
    public GremlinScriptContext(final Reader in, final Writer out, final Writer error) {
        engineScope = new SimpleBindings();
        globalScope = null;
        reader = in;
        writer = out;
        errorWriter = error;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setBindings(final Bindings bindings, final int scope) {
        switch (scope) {
            case ENGINE_SCOPE:
                if (null == bindings) throw new NullPointerException("Engine scope cannot be null.");
                engineScope = bindings;
                break;
            case GLOBAL_SCOPE:
                globalScope = bindings;
                break;
            default:
                throw new IllegalArgumentException("Invalid scope value.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getAttribute(final String name) {
        checkName(name);
        if (engineScope.containsKey(name)) {
            return getAttribute(name, ENGINE_SCOPE);
        } else if (globalScope != null && globalScope.containsKey(name)) {
            return getAttribute(name, GLOBAL_SCOPE);
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getAttribute(final String name, final int scope) {
        checkName(name);
        switch (scope) {
            case ENGINE_SCOPE:
                return engineScope.get(name);
            case GLOBAL_SCOPE:
                return globalScope != null ? globalScope.get(name) : null;
            default:
                throw new IllegalArgumentException("Illegal scope value.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object removeAttribute(final String name, final int scope) {
        checkName(name);
        switch (scope) {
            case ENGINE_SCOPE:
                return getBindings(ENGINE_SCOPE) != null ? getBindings(ENGINE_SCOPE).remove(name) : null;
            case GLOBAL_SCOPE:
                return getBindings(GLOBAL_SCOPE) != null ? getBindings(GLOBAL_SCOPE).remove(name) : null;
            default:
                throw new IllegalArgumentException("Illegal scope value.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAttribute(final String name, final Object value, final int scope) {
        checkName(name);
        switch (scope) {
            case ENGINE_SCOPE:
                engineScope.put(name, value);
                return;
            case GLOBAL_SCOPE:
                if (globalScope != null) globalScope.put(name, value);
                return;
            default:
                throw new IllegalArgumentException("Illegal scope value.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getAttributesScope(final String name) {
        checkName(name);
        if (engineScope.containsKey(name))
            return ENGINE_SCOPE;
        else if (globalScope != null && globalScope.containsKey(name))
            return GLOBAL_SCOPE;
        else
            return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Bindings getBindings(final int scope) {
        if (scope == ENGINE_SCOPE)
            return engineScope;
        else if (scope == GLOBAL_SCOPE)
            return globalScope;
        else
            throw new IllegalArgumentException("Illegal scope value.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Integer> getScopes() {
        return scopes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writer getWriter() {
        return writer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reader getReader() {
        return reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReader(final Reader reader) {
        this.reader = reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWriter(final Writer writer) {
        this.writer = writer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writer getErrorWriter() {
        return errorWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setErrorWriter(final Writer writer) {
        this.errorWriter = writer;
    }

    private void checkName(final String name) {
        Objects.requireNonNull(name);
        if (name.isEmpty()) throw new IllegalArgumentException("name cannot be empty");
    }
}
