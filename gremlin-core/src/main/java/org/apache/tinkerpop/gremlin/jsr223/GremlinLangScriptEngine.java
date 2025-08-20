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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinAntlrToJava;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinQueryParser;
import org.apache.tinkerpop.gremlin.language.grammar.VariableResolver;
import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A {@link GremlinScriptEngine} implementation that evaluates Gremlin scripts using {@code gremlin-language}. As it
 * uses {@code gremlin-language} and thus the ANTLR parser, it is not capable of process arbitrary scripts as the
 * {@code GremlinGroovyScriptEngine} can and is therefore a more secure Gremlin evaluator. It is obviously restricted
 * to the capabilities of the ANTLR grammar so therefore syntax that includes things like lambdas are not supported.
 * <p/>
 * As an internal note, technically, this is an incomplete implementation of the {@link GremlinScriptEngine} in the
 * traditional sense as a drop-in replacement for something like the {@code GremlinGroovyScriptEngine}. As a result,
 * this {@link GremlinScriptEngine} cannot pass the {@code GremlinScriptEngineSuite} tests in full. On the other hand,
 * this limitation is precisely what makes this implementation better from a security perspective. Ultimately, this
 * implementation represents the first step to changes in what it means to have a {@link GremlinScriptEngine}. In some
 * sense, there is question why a {@link GremlinScriptEngine} approach is necessary at all except for easily plugging
 * into the existing internals of Gremlin Server or more specifically the {@code GremlinExecutor}.
 * <p/>
 * The engine includes a cache that can help spare repeated parsing of the same traversal. It is keyed to each
 * configuration of the {@link GraphTraversalSource} that initially requested its execution via the bindings given to
 * the {@code eval} method. Each cache is keyed on the original Gremlin string and holds the initialized parsed
 * traversal for its value. If that Gremlin string is matched again in the future, it will use the one from the cache
 * rather than parse again via the ANTLR grammar. In addition, if {@link GValue} instances were used in formation of
 * the traversal, it will substitute in new bindings given with the {@code eval}.
 */
public class GremlinLangScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {
    private volatile GremlinScriptEngineFactory factory;

    private final Function<Map<String, Object>, VariableResolver> variableResolverMaker;

    private final Map<GraphTraversalSource, Cache<String, Traversal.Admin<?,?>>> traversalCaches = new ConcurrentHashMap<>();

    /**
     * Determines if the traversal cache is enabled or not. This is {@code false} by default.
     */
    private final boolean cacheEnabled;

    private final Caffeine<String, Traversal<?,?>> caffeine;

    /**
     * Creates a new instance using no {@link Customizer}.
     */
    public GremlinLangScriptEngine() {
        this(new Customizer[0]);
    }

    public GremlinLangScriptEngine(final Customizer... customizers) {
        final List<Customizer> listOfCustomizers = Arrays.asList(customizers);

        // this ScriptEngine really only supports the VariableResolverCustomizer to configure the VariableResolver
        // and can't configure it more than once. first one wins
        final Optional<Customizer> opt = listOfCustomizers.stream().
                filter(c -> c instanceof VariableResolverCustomizer).findFirst();
        variableResolverMaker = opt.isPresent() ?
                ((VariableResolverCustomizer) opt.get()).getVariableResolverMaker() :
                VariableResolver.DirectVariableResolver::new;

        // cache customization
        final Optional<GremlinLangCustomizer> gremlinLangCustomizer = listOfCustomizers.stream().
                filter(c -> c instanceof GremlinLangCustomizer).
                map(c -> (GremlinLangCustomizer) c).findFirst();
        if (gremlinLangCustomizer.isPresent()) {
            final GremlinLangCustomizer customizer = gremlinLangCustomizer.get();
            cacheEnabled = customizer.isCacheEnabled();
            caffeine = customizer.getCacheMaker();
        } else {
            cacheEnabled = false;
            caffeine = null;
        }
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
     * Gremlin scripts evaluated by the grammar must be bound to "g" and should evaluate to a "g" in the
     * {@code ScriptContext} that is of type {@link TraversalSource}
     */
    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        final Object o = context.getAttribute("g");
        if (!(o instanceof GraphTraversalSource))
            throw new IllegalArgumentException("g is of type " + o.getClass().getSimpleName() + " and is not an instance of TraversalSource");

        final GraphTraversalSource g = (GraphTraversalSource) o;
        final Map<String, Object> m = context.getBindings(ScriptContext.ENGINE_SCOPE);

        Cache<String, Traversal.Admin<?, ?>> traversalCache = null;
        if (cacheEnabled) {
            // find the property cache for the g that was passed in so that we have the right Graph instance/context
            traversalCache = traversalCaches.computeIfAbsent(g, k -> caffeine.build());

            // if found in cache, clone the traversal, apply new bindings and then return the clone
            final Traversal cachedTraversal = traversalCache.getIfPresent(script);
            if (cachedTraversal != null) {
                final Traversal.Admin<?, ?> clonedTraversal = cachedTraversal.asAdmin().clone();
                final GValueManager manager = clonedTraversal.getGValueManager();
                final Set<String> variables = manager.getVariableNames();

                // every variable the traversal has should match a binding
                for (String variable : variables) {
                    if (!m.containsKey(variable)) {
                        throw new IllegalArgumentException(variable + " binding is not found");
                    }

                    manager.updateVariable(variable, m.get(variable));
                }

                return clonedTraversal;
            }
        }

        // if not in cache or cache not enabled, parse the script
        final GremlinAntlrToJava antlr = new GremlinAntlrToJava((GraphTraversalSource) o,
                variableResolverMaker.apply(m));

        try {
            // parse the script
            final Object result = GremlinQueryParser.parse(script, antlr);

            // add the traversal to the cache in its executable form - note that the cache is really just sparing us
            // script parsing
            // Note that caching is skipped if the result is not a Traversal (ie the script is terminated with next())
            if (cacheEnabled && result instanceof Traversal.Admin) traversalCache.put(script, ((Traversal.Admin<?,?>) result).clone());

            return result;
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
