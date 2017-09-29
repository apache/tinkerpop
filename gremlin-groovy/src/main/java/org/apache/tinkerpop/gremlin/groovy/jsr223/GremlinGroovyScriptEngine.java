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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.lang.Tuple;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.jsr223.CoreGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptContext;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.codehaus.groovy.ast.ClassHelper;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.jsr223.GroovyCompiledScript;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.runtime.MetaClassHelper;
import org.codehaus.groovy.runtime.MethodClosure;
import org.codehaus.groovy.util.ReferenceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Provides methods to compile and evaluate Gremlin scripts. Compiled scripts are stored in a managed cache to cut
 * down on compilation times of future evaluations of the same script.  This {@code ScriptEngine} implementation is
 * heavily adapted from the {@code GroovyScriptEngineImpl} to include some additional functionality.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @see org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor
 */
public class GremlinGroovyScriptEngine extends GroovyScriptEngineImpl
        implements AutoCloseable, GremlinScriptEngine {

    private static final Logger log = LoggerFactory.getLogger(GremlinGroovyScriptEngine.class);
    /**
     * An "internal" key for sandboxing the script engine - technically not for public use.
     */
    public static final String COMPILE_OPTIONS_VAR_TYPES = "sandbox.bindings";

    /**
     * The attribute key (passed as a binding on the context) for how to cache scripts.  The value must be one of
     * the following:
     * <ul>
     * <li>{@link #REFERENCE_TYPE_HARD}</li>
     * <li>{@link #REFERENCE_TYPE_SOFT}</li>
     * <li>{@link #REFERENCE_TYPE_WEAK}</li>
     * <li>{@link #REFERENCE_TYPE_PHANTOM}</li>
     * </ul>
     */
    public static final String KEY_REFERENCE_TYPE = "#jsr223.groovy.engine.keep.globals";

    /**
     * A value to the {@link #KEY_REFERENCE_TYPE} that immediately garbage collects the script after evaluation.
     */
    public static final String REFERENCE_TYPE_PHANTOM = "phantom";

    /**
     * A value to the {@link #KEY_REFERENCE_TYPE} that marks the script as one that can be garbage collected
     * even when memory is abundant.
     */
    public static final String REFERENCE_TYPE_WEAK = "weak";

    /**
     * A value to the {@link #KEY_REFERENCE_TYPE} that retains the script until memory is "low" and therefore
     * should be reclaimed before an {@link OutOfMemoryError} occurs.
     */
    public static final String REFERENCE_TYPE_SOFT = "soft";

    /**
     * A value to the {@link #KEY_REFERENCE_TYPE} that makes the evaluated script available in the cache for the life
     * of the JVM.
     */
    public static final String REFERENCE_TYPE_HARD = "hard";

    /**
     * Name of variable that holds local variables to be globally bound if "interpreter mode" is enabled with
     * {@link InterpreterModeGroovyCustomizer}.
     */
    public static final String COLLECTED_BOUND_VARS_MAP_VARNAME = "gremlin_script_engine_collected_boundvars";

    private static final Pattern patternImportStatic = Pattern.compile("\\Aimport\\sstatic.*");

    public static final ThreadLocal<Map<String, Object>> COMPILE_OPTIONS = new ThreadLocal<Map<String, Object>>() {
        @Override
        protected Map<String, Object> initialValue() {
            return new HashMap<>();
        }
    };

    private GremlinGroovyClassLoader loader;

    /**
     * Script to generated Class map.
     */
    private final LoadingCache<String, Future<Class>> classMap = Caffeine.newBuilder().
            softValues().
            recordStats().
            build(new CacheLoader<String, Future<Class>>() {
        @Override
        public Future<Class> load(final String script) throws Exception {
            final long start = System.currentTimeMillis();

            return CompletableFuture.supplyAsync(() -> {
                try {
                    return loader.parseClass(script, generateScriptName());
                } catch (CompilationFailedException e) {
                    final long finish = System.currentTimeMillis();
                    log.error("Script compilation FAILED {} took {}ms {}", script, finish - start, e);
                    failedCompilationCount.incrementAndGet();
                    throw e;
                } finally {
                    final long time = System.currentTimeMillis() - start;
                    if (time > expectedCompilationTime) {
                        //We warn if a script took longer than a few seconds. Repeatedly seeing these warnings is a sign that something is wrong.
                        //Scripts with a large numbers of parameters often trigger this and should be avoided.
                        log.warn("Script compilation {} took {}ms", script, time);
                        longRunCompilationCount.incrementAndGet();
                    } else {
                        log.debug("Script compilation {} took {}ms", script, time);
                    }
                }
            }, Runnable::run);
        }
    });

    /**
     * Global closures map - this is used to simulate a single global functions namespace
     */
    private final ManagedConcurrentValueMap<String, Closure> globalClosures = new ManagedConcurrentValueMap<>(ReferenceBundle.getHardBundle());

    /**
     * Ensures unique script names across all instances.
     */
    private final static AtomicLong scriptNameCounter = new AtomicLong(0L);

    /**
     * A counter for the instance that tracks the number of warnings issued during script compilations that exceeded
     * the {@link #expectedCompilationTime}.
     */
    private final AtomicLong longRunCompilationCount = new AtomicLong(0L);

    /**
     * A counter for the instance that tracks the number of failed compilations. Note that the failures need to be
     * tracked outside of cache failure load stats because the loading mechanism will always successfully return
     * a future and won't trigger a failure.
     */
    private final AtomicLong failedCompilationCount = new AtomicLong(0L);

    /**
     * The list of loaded plugins for the console.
     */
    private final Set<String> loadedPlugins = new HashSet<>();

    private volatile GremlinGroovyScriptEngineFactory factory;

    private static final String STATIC = "static";
    private static final String SCRIPT = "Script";
    private static final String DOT_GROOVY = ".groovy";
    private static final String GROOVY_LANG_SCRIPT = "groovy.lang.Script";

    private final ImportGroovyCustomizer importGroovyCustomizer;
    private final List<GroovyCustomizer> groovyCustomizers;

    private final boolean interpreterModeEnabled;
    private final long expectedCompilationTime;

    /**
     * There is no need to require type checking infrastructure if type checking is not enabled.
     */
    private final boolean typeCheckingEnabled;

    /**
     * Creates a new instance using no {@link Customizer}.
     */
    public GremlinGroovyScriptEngine() {
        this(new Customizer[0]);
    }

    public GremlinGroovyScriptEngine(final Customizer... customizers) {
        // initialize the global scope in case this scriptengine was instantiated outside of the ScriptEngineManager
        setBindings(new ConcurrentBindings(), ScriptContext.GLOBAL_SCOPE);

        final List<Customizer> listOfCustomizers = new ArrayList<>(Arrays.asList(customizers));

        // always need this plugin for a scriptengine to be "Gremlin-enabled"
        CoreGremlinPlugin.instance().getCustomizers("gremlin-groovy").ifPresent(c -> listOfCustomizers.addAll(Arrays.asList(c)));

        GremlinLoader.load();

        final List<ImportCustomizer> importCustomizers = listOfCustomizers.stream()
                .filter(p -> p instanceof ImportCustomizer)
                .map(p -> (ImportCustomizer) p)
                .collect(Collectors.toList());
        final ImportCustomizer[] importCustomizerArray = new ImportCustomizer[importCustomizers.size()];
        importGroovyCustomizer = new ImportGroovyCustomizer(importCustomizers.toArray(importCustomizerArray));

        groovyCustomizers = listOfCustomizers.stream()
                .filter(p -> p instanceof GroovyCustomizer)
                .map(p -> ((GroovyCustomizer) p))
                .collect(Collectors.toList());

        final Optional<CompilationOptionsCustomizer> compilationOptionsCustomizerProvider = listOfCustomizers.stream()
                .filter(p -> p instanceof CompilationOptionsCustomizer)
                .map(p -> (CompilationOptionsCustomizer) p).findFirst();
        expectedCompilationTime = compilationOptionsCustomizerProvider.isPresent() ?
                compilationOptionsCustomizerProvider.get().getExpectedCompilationTime() : 5000;

        typeCheckingEnabled = listOfCustomizers.stream()
                .anyMatch(p -> p instanceof TypeCheckedGroovyCustomizer || p instanceof CompileStaticGroovyCustomizer);

        // determine if interpreter mode should be enabled
        interpreterModeEnabled = groovyCustomizers.stream()
                .anyMatch(p -> p.getClass().equals(InterpreterModeGroovyCustomizer.class));

        createClassLoader();
    }

    /**
     * Get the list of loaded plugins.
     */
    public Set getPlugins() {
        return loadedPlugins;
    }

    @Override
    public Traversal.Admin eval(final Bytecode bytecode, final Bindings bindings, final String traversalSource) throws ScriptException {
        // these validations occur before merging in bytecode bindings which will override existing ones. need to
        // extract the named traversalsource prior to that happening so that bytecode bindings can share the same
        // namespace as global bindings (e.g. traversalsources and graphs).
        if (traversalSource.equals(HIDDEN_G))
            throw new IllegalArgumentException("The traversalSource cannot have the name " + HIDDEN_G+ " - it is reserved");

        if (bindings.containsKey(HIDDEN_G))
            throw new IllegalArgumentException("Bindings cannot include " + HIDDEN_G + " - it is reserved");

        if (!bindings.containsKey(traversalSource))
            throw new IllegalArgumentException("The bindings available to the ScriptEngine do not contain a traversalSource named: " + traversalSource);

        final Object b = bindings.get(traversalSource);
        if (!(b instanceof TraversalSource))
            throw new IllegalArgumentException(traversalSource + " is of type " + b.getClass().getSimpleName() + " and is not an instance of TraversalSource");

        final Bindings inner = new SimpleBindings();
        inner.putAll(bindings);
        inner.putAll(bytecode.getBindings());
        inner.put(HIDDEN_G, b);

        return (Traversal.Admin) this.eval(GroovyTranslator.of(HIDDEN_G).translate(bytecode), inner);
    }

    /**
     * @deprecated As of release 3.2.4, not replaced as this class will not implement {@code AutoCloseable} in the
     * future.
     */
    @Override
    @Deprecated
    public void close() throws Exception {
    }

    /**
     * Resets the entire {@code GremlinGroovyScriptEngine} by clearing script caches, recreating the classloader,
     * clearing bindings.
     */
    public void reset() {
        internalReset();
        getContext().getBindings(ScriptContext.ENGINE_SCOPE).clear();
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
            ctxt.setBindings(nn,
                    ScriptContext.ENGINE_SCOPE);
        } else {
            throw new NullPointerException("Engine scope Bindings may not be null.");
        }

        return ctxt;
    }

    /**
     * Resets the {@code ScriptEngine} but does not clear the loaded plugins or bindings.
     */
    private void internalReset() {
        createClassLoader();

        // must clear the local cache here because the classloader has been reset.  therefore, classes previously
        // referenced before that might not have evaluated might cleanly evaluate now.
        classMap.invalidateAll();
        globalClosures.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return eval(readFully(reader), context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        try {
            final String val = (String) context.getAttribute(KEY_REFERENCE_TYPE, ScriptContext.ENGINE_SCOPE);
            ReferenceBundle bundle = ReferenceBundle.getHardBundle();
            if (val != null && val.length() > 0) {
                if (val.equalsIgnoreCase(REFERENCE_TYPE_SOFT)) {
                    bundle = ReferenceBundle.getSoftBundle();
                } else if (val.equalsIgnoreCase(REFERENCE_TYPE_WEAK)) {
                    bundle = ReferenceBundle.getWeakBundle();
                } else if (val.equalsIgnoreCase(REFERENCE_TYPE_PHANTOM)) {
                    bundle = ReferenceBundle.getPhantomBundle();
                }
            }
            globalClosures.setBundle(bundle);
        } catch (ClassCastException cce) { /*ignore.*/ }

        try {
            registerBindingTypes(context);
            final Class clazz = getScriptClass(script);
            if (null == clazz) throw new ScriptException("Script class is null");
            return eval(clazz, context);
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    /**
     * Create bindings to be used by this {@code ScriptEngine}.  In this case, {@link SimpleBindings} are returned.
     */
    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GremlinScriptEngineFactory getFactory() {
        if (factory == null) {
            synchronized (this) {
                if (factory == null) {
                    factory = new GremlinGroovyScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompiledScript compile(final String scriptSource) throws ScriptException {
        try {
            return new GroovyCompiledScript(this, getScriptClass(scriptSource));
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompiledScript compile(final Reader reader) throws ScriptException {
        return compile(readFully(reader));
    }

    @Override
    public Object invokeFunction(final String name, final Object... args) throws ScriptException, NoSuchMethodException {
        return invokeImpl(null, name, args);
    }

    @Override
    public Object invokeMethod(final Object thiz, final String name, final Object... args) throws ScriptException, NoSuchMethodException {
        if (thiz == null) {
            throw new IllegalArgumentException("Script object can not be null");
        } else {
            return invokeImpl(thiz, name, args);
        }
    }

    @Override
    public <T> T getInterface(final Class<T> clazz) {
        return makeInterface(null, clazz);
    }

    @Override
    public <T> T getInterface(final Object thiz, final Class<T> clazz) {
        if (null == thiz) throw new IllegalArgumentException("script object is null");

        return makeInterface(thiz, clazz);
    }

    /**
     * Gets the number of compilations that extended beyond the {@link #expectedCompilationTime}.
     */
    public long getClassCacheLongRunCompilationCount() {
        return longRunCompilationCount.longValue();
    }

    /**
     * Gets the estimated size of the class cache for compiled scripts.
     */
    public long getClassCacheEstimatedSize() {
        return classMap.estimatedSize();
    }

    /**
     * Gets the average time spent compiling new scripts.
     */
    public double getClassCacheAverageLoadPenalty() {
        return classMap.stats().averageLoadPenalty();
    }

    /**
     * Gets the number of times a script compiled to a class has been evicted from the cache.
     */
    public long getClassCacheEvictionCount() {
        return classMap.stats().evictionCount();
    }

    /**
     * Gets the sum of the weights of evicted entries from the class cache.
     */
    public long getClassCacheEvictionWeight() {
        return classMap.stats().evictionWeight();
    }

    /**
     * Gets the number of times cache look up for a compiled script returned a cached value.
     */
    public long getClassCacheHitCount() {
        return classMap.stats().hitCount();
    }

    /**
     * Gets the hit rate of the class cache.
     */
    public double getClassCacheHitRate() {
        return classMap.stats().hitRate();
    }

    /**
     * Gets the total number of times the cache lookup method attempted to compile new scripts.
     */
    public long getClassCacheLoadCount() {
        return classMap.stats().loadCount();
    }

    /**
     * Gets the total number of times the cache lookup method failed to compile a new script.
     */
    public long getClassCacheLoadFailureCount() {
        // don't use classMap.stats().loadFailureCount() because the load mechanism always succeeds with a future
        // that will in turn contain the success or failure
        return failedCompilationCount.longValue();
    }

    /**
     * Gets the ratio of script compilation attempts that failed.
     */
    public double getClassCacheLoadFailureRate() {
        // don't use classMap.stats().loadFailureRate() because the load mechanism always succeeds with a future
        // that will in turn contain the success or failure
        long totalLoadCount = classMap.stats().loadCount();
        return (totalLoadCount == 0)
                ? 0.0
                : (double) failedCompilationCount.longValue() / totalLoadCount;
    }

    /**
     * Gets the total number of times the cache lookup method succeeded to compile a new script.
     */
    public long getClassCacheLoadSuccessCount() {
        // don't use classMap.stats().loadSuccessCount() because the load mechanism always succeeds with a future
        // that will in turn contain the success or failure
        return classMap.stats().loadCount() - failedCompilationCount.longValue();
    }

    /**
     * Gets the total number of times the cache lookup method returned a newly compiled script.
     */
    public long getClassCacheMissCount() {
        return classMap.stats().missCount();
    }

    /**
     * Gets the ratio of script compilation attempts that were misses.
     */
    public double getClassCacheMissRate() {
        return classMap.stats().missRate();
    }

    /**
     * Gets the total number of times the cache lookup method returned a cached or uncached value.
     */
    public long getClassCacheRequestCount() {
        return classMap.stats().missCount();
    }

    /**
     * Gets the total number of nanoseconds that the cache spent compiling scripts.
     */
    public long getClassCacheTotalLoadTime() {
        return classMap.stats().totalLoadTime();
    }

    Class getScriptClass(final String script) throws Exception {
        try {
            return classMap.get(script).get();
        } catch (ExecutionException e) {
            final Throwable t = e.getCause();

            // more often than not the cause is a compilation problem but there might be other failures that can
            // occur in which case, just throw the ExecutionException as-is and let it bubble up as i'm not sure
            // what the specific handling should be
            if (t instanceof CompilationFailedException)
                throw (CompilationFailedException) t;
            else
                throw e;
        } catch (InterruptedException e) {
            //This should never happen as the future should completed before it is returned to the us.
            throw new AssertionError();
        }
    }

    boolean isCached(final String script) {
        return classMap.getIfPresent(script) != null;
    }

    Object eval(final Class scriptClass, final ScriptContext context) throws ScriptException {
        final Binding binding = new Binding(context.getBindings(ScriptContext.ENGINE_SCOPE)) {
            @Override
            public Object getVariable(String name) {
                synchronized (context) {
                    int scope = context.getAttributesScope(name);
                    if (scope != -1) {
                        return context.getAttribute(name, scope);
                    }
                    // Redirect script output to context writer, if out var is not already provided
                    if ("out".equals(name)) {
                        final Writer writer = context.getWriter();
                        if (writer != null) {
                            return (writer instanceof PrintWriter) ?
                                    (PrintWriter) writer :
                                    new PrintWriter(writer, true);
                        }
                    }
                    // Provide access to engine context, if context var is not already provided
                    if ("context".equals(name)) {
                        return context;
                    }
                }
                throw new MissingPropertyException(name, getClass());
            }

            @Override
            public void setVariable(String name, Object value) {
                synchronized (context) {
                    int scope = context.getAttributesScope(name);
                    if (scope == -1) {
                        scope = ScriptContext.ENGINE_SCOPE;
                    }
                    context.setAttribute(name, value, scope);
                }
            }
        };

        try {
            // if this class is not an instance of Script, it's a full-blown class then simply return that class
            if (!Script.class.isAssignableFrom(scriptClass)) {
                return scriptClass;
            } else {
                final Script scriptObject = InvokerHelper.createScript(scriptClass, binding);
                for (Method m : scriptClass.getMethods()) {
                    final String name = m.getName();
                    globalClosures.put(name, new MethodClosure(scriptObject, name));
                }

                final MetaClass oldMetaClass = scriptObject.getMetaClass();
                scriptObject.setMetaClass(new DelegatingMetaClass(oldMetaClass) {
                    @Override
                    public Object invokeMethod(final Object object, final String name, final Object args) {
                        if (args == null) {
                            return invokeMethod(object, name, MetaClassHelper.EMPTY_ARRAY);
                        } else if (args instanceof Tuple) {
                            return invokeMethod(object, name, ((Tuple) args).toArray());
                        } else if (args instanceof Object[]) {
                            return invokeMethod(object, name, (Object[]) args);
                        } else {
                            return invokeMethod(object, name, new Object[]{args});
                        }
                    }

                    @Override
                    public Object invokeMethod(final Object object, final String name, final Object args[]) {
                        try {
                            return super.invokeMethod(object, name, args);
                        } catch (MissingMethodException mme) {
                            return callGlobal(name, args, context);
                        }
                    }

                    @Override
                    public Object invokeStaticMethod(final Object object, final String name, final Object args[]) {
                        try {
                            return super.invokeStaticMethod(object, name, args);
                        } catch (MissingMethodException mme) {
                            return callGlobal(name, args, context);
                        }
                    }
                });

                final Object o = scriptObject.run();

                // if interpreter mode is enable then local vars of the script are promoted to engine scope bindings.
                if (interpreterModeEnabled) {
                    final Map<String, Object> localVars = (Map<String, Object>) context.getAttribute(COLLECTED_BOUND_VARS_MAP_VARNAME);
                    if (localVars != null) {
                        localVars.entrySet().forEach(e -> {
                            // closures need to be cached for later use
                            if (e.getValue() instanceof Closure)
                                globalClosures.put(e.getKey(), (Closure) e.getValue());

                            context.setAttribute(e.getKey(), e.getValue(), ScriptContext.ENGINE_SCOPE);
                        });

                        // get rid of the temporary collected vars
                        context.removeAttribute(COLLECTED_BOUND_VARS_MAP_VARNAME, ScriptContext.ENGINE_SCOPE);
                        localVars.clear();
                    }
                }

                return o;
            }
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    private void registerBindingTypes(final ScriptContext context) {
        if (typeCheckingEnabled) {
            final Map<String, ClassNode> variableTypes = new HashMap<>();
            clearVarTypes();

            // use null for the classtype if the binding value itself is null - not fully sure if that is
            // a sound way to deal with that.  didn't see a class type for null - maybe it should just be
            // unknown and be "Object".  at least null is properly being accounted for now.
            context.getBindings(ScriptContext.GLOBAL_SCOPE).forEach((k, v) ->
                    variableTypes.put(k, null == v ? null : ClassHelper.make(v.getClass())));
            context.getBindings(ScriptContext.ENGINE_SCOPE).forEach((k, v) ->
                    variableTypes.put(k, null == v ? null : ClassHelper.make(v.getClass())));

            COMPILE_OPTIONS.get().put(COMPILE_OPTIONS_VAR_TYPES, variableTypes);
        }
    }

    private static void clearVarTypes() {
        final Map<String, Object> m = COMPILE_OPTIONS.get();
        if (m.containsKey(COMPILE_OPTIONS_VAR_TYPES))
            ((Map<String, ClassNode>) m.get(COMPILE_OPTIONS_VAR_TYPES)).clear();
    }

    private Object invokeImpl(final Object thiz, final String name, final Object args[]) throws ScriptException, NoSuchMethodException {
        if (name == null) {
            throw new NullPointerException("Method name can not be null");
        }
        try {
            if (thiz != null) {
                return InvokerHelper.invokeMethod(thiz, name, args);
            }
        } catch (MissingMethodException mme) {
            throw new NoSuchMethodException(mme.getMessage());
        } catch (Exception e) {
            throw new ScriptException(e);
        }
        return callGlobal(name, args);
    }

    private synchronized void createClassLoader() {
        final CompilerConfiguration conf = new CompilerConfiguration(CompilerConfiguration.DEFAULT);
        conf.addCompilationCustomizers(this.importGroovyCustomizer.create());

        // ConfigurationCustomizerProvider is treated separately
        groovyCustomizers.stream().filter(cp -> !(cp instanceof ConfigurationGroovyCustomizer))
                .forEach(p -> conf.addCompilationCustomizers(p.create()));

        groovyCustomizers.stream().filter(cp -> cp instanceof ConfigurationGroovyCustomizer).findFirst()
                .ifPresent(cp -> ((ConfigurationGroovyCustomizer) cp).applyCustomization(conf));

        this.loader = new GremlinGroovyClassLoader(getParentLoader(), conf);
    }

    private Object callGlobal(final String name, final Object args[]) {
        return callGlobal(name, args, context);
    }

    private Object callGlobal(final String name, final Object args[], final ScriptContext ctx) {
        final Closure closure = globalClosures.get(name);
        if (closure != null) {
            return closure.call(args);
        }

        final Object value = ctx.getAttribute(name);
        if (value instanceof Closure) {
            return ((Closure) value).call(args);
        } else {
            throw new MissingMethodException(name, getClass(), args);
        }
    }

    private synchronized String generateScriptName() {
        return SCRIPT + scriptNameCounter.incrementAndGet() + DOT_GROOVY;
    }

    @SuppressWarnings("unchecked")
    private <T> T makeInterface(final Object obj, final Class<T> clazz) {
        if (null == clazz || !clazz.isInterface()) throw new IllegalArgumentException("interface Class expected");

        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[]{clazz},
                (proxy, m, args) -> invokeImpl(obj, m.getName(), args));
    }

    protected ClassLoader getParentLoader() {
        final ClassLoader ctxtLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Class c = ctxtLoader.loadClass(GROOVY_LANG_SCRIPT);
            if (c == groovy.lang.Script.class) {
                return ctxtLoader;
            }
        } catch (ClassNotFoundException ignored) {
        }
        return groovy.lang.Script.class.getClassLoader();
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