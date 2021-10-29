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

import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.Reader;

/**
 * A {@link GremlinScriptEngine} implementation that evaluates Gremlin scripts using {@code gremlin-language}. As it
 * uses {@code gremlin-language} and thus the ANTLR parser, it is not capable of process arbitrary scripts as the
 * {@code GremlinGroovyScriptEngine} can and is therefore a more secure Gremlin evaluator. It is obviously restricted
 * to the capabilities of the ANTLR grammar so therefore syntax that includes things like lambdas are not supported.
 * For bytecode evaluation it simply uses the {@link JavaTranslator}.
 * <p/>
 * As an internal note, technically, this is an incomplete implementation of the {@link GremlinScriptEngine} in the
 * traditional sense as a drop-in replacement for something like the {@code GremlinGroovyScriptEngine}. As a result,
 * this {@link GremlinScriptEngine} cannot pass the {@code GremlinScriptEngineSuite} tests in full. On the other hand,
 * this limitation is precisely what makes this implementation better from a security perspective. Ultimately, this
 * implementation represents the first step to changes in what it means to have a {@link GremlinScriptEngine}. In some
 * sense, there is question why a {@link GremlinScriptEngine} approach is necessary at all except for easily plugging
 * into the existing internals of Gremlin Server or more specifically the {@code GremlinExecutor}.
 */
public class GremlinLangScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
    private volatile GremlinScriptEngineFactory factory;

    /**
     * Creates a new instance using no {@link Customizer}.
     */
    public GremlinLangScriptEngine() {
        this(new Customizer[0]);
    }

    public GremlinLangScriptEngine(final Customizer... customizers) {
    }

    @Override
    public GremlinScriptEngineFactory getFactory() {
        if (factory == null) {
            synchronized (this) {
                if (factory == null) {
                    factory = new GremlinLangScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }

    /**
     * Bytecode is evaluated by the {@link JavaTranslator}.
     */
    @Override
    public Traversal.Admin eval(final Bytecode bytecode, final Bindings bindings, final String traversalSource) throws ScriptException {
        if (traversalSource.equals(HIDDEN_G))
            throw new IllegalArgumentException("The traversalSource cannot have the name " + HIDDEN_G + " - it is reserved");

        if (bindings.containsKey(HIDDEN_G))
            throw new IllegalArgumentException("Bindings cannot include " + HIDDEN_G + " - it is reserved");

        if (!bindings.containsKey(traversalSource))
            throw new IllegalArgumentException("The bindings available to the ScriptEngine do not contain a traversalSource named: " + traversalSource);

        final Object b = bindings.get(traversalSource);
        if (!(b instanceof TraversalSource))
            throw new IllegalArgumentException(traversalSource + " is of type " + b.getClass().getSimpleName() + " and is not an instance of TraversalSource");

        return JavaTranslator.of((TraversalSource) b).translate(bytecode);
    }

    /**
     * Gremlin scripts evaluated by the grammar must be bound to "g" and should evaluate to a "g" in the
     * {@code ScriptContext} that is of type {@link TraversalSource}
     */
    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        final Object o = context.getAttribute("g");
        if (!(o instanceof GraphTraversalSource))
            throw new IllegalArgumentException("g is of type " + o.getClass().getSimpleName() + " and is not an instance of TraversalSource");

        final GremlinAntlrToJava antlr = new GremlinAntlrToJava((GraphTraversalSource) o);

        try {
            return GremlinQueryParser.parse(script, antlr);
        } catch (Exception ex) {
            throw new ScriptException(ex);
        }
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return eval(readFully(reader), context);
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    /**
     * Creates the {@code ScriptContext} using a {@link GremlinScriptContext} which avoids a significant amount of
     * additional object creation on script evaluation.
     */
    @Override
    protected ScriptContext getScriptContext(final Bindings nn) {
        final GremlinScriptContext ctxt = new GremlinScriptContext(context.getReader(), context.getWriter(), context.getErrorWriter());
        final Bindings gs = getBindings(ScriptContext.GLOBAL_SCOPE);

        if (gs != null) ctxt.setBindings(gs, ScriptContext.GLOBAL_SCOPE);

        if (nn != null) {
            ctxt.setBindings(nn, ScriptContext.ENGINE_SCOPE);
        } else {
            throw new NullPointerException("Engine scope Bindings may not be null.");
        }

        return ctxt;
    }

    private String readFully(final Reader reader) throws ScriptException {
        final char arr[] = new char[8192];
        final StringBuilder buf = new StringBuilder();
        int numChars;
        try {
            while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
                buf.append(arr, 0, numChars);
            }
        } catch (IOException exp) {
            throw new ScriptException(exp);
        }
        return buf.toString();
    }
}
