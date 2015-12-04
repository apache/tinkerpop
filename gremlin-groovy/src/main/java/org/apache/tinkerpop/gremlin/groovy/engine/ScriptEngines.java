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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.DependencyManager;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngineFactory;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Holds a batch of the configured {@code ScriptEngine} objects for the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ScriptEngines implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ScriptEngines.class);

    private final static ScriptEngineManager SCRIPT_ENGINE_MANAGER = new ScriptEngineManager();
    private static final GremlinGroovyScriptEngineFactory gremlinGroovyScriptEngineFactory = new GremlinGroovyScriptEngineFactory();

    /**
     * {@code ScriptEngine} objects configured for the server keyed on the language name.
     */
    private final Map<String, ScriptEngine> scriptEngines = new ConcurrentHashMap<>();

    private final AtomicBoolean controlOperationExecuting = new AtomicBoolean(false);
    private final Queue<Thread> controlWaiters = new ConcurrentLinkedQueue<>();
    private final Queue<Thread> evalWaiters = new ConcurrentLinkedQueue<>();

    private final Consumer<ScriptEngines> initializer;

    /**
     * Creates a new object.
     *
     * @param initializer allows for external initialization of the newly created {@code ScriptEngines} object
     */
    public ScriptEngines(final Consumer<ScriptEngines> initializer) {
        this.initializer = initializer;
        this.initializer.accept(this);
    }

    /**
     * Evaluate a script with {@code Bindings} for a particular language.
     */
    public Object eval(final String script, final Bindings bindings, final String language) throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException(String.format("Language [%s] not supported", language));

        awaitControlOp();

        final ScriptEngine engine = scriptEngines.get(language);
        final Bindings all = mergeBindings(bindings, engine);

        return engine.eval(script, all);
    }

    /**
     * Evaluate a script with {@code Bindings} for a particular language.
     */
    public Object eval(final Reader reader, final Bindings bindings, final String language)
            throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException("Language [%s] not supported");

        awaitControlOp();

        final ScriptEngine engine = scriptEngines.get(language);
        final Bindings all = mergeBindings(bindings, engine);

        return engine.eval(reader, all);
    }

    /**
     * Compiles a script without executing it.
     *
     * @throws UnsupportedOperationException if the {@link ScriptEngine} implementation does not implement
     * the {@link javax.script.Compilable} interface.
     */
    public CompiledScript compile(final String script, final String language) throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException("Language [%s] not supported");

        awaitControlOp();
        final ScriptEngine scriptEngine = scriptEngines.get(language);
        if (!Compilable.class.isAssignableFrom(scriptEngine.getClass()))
            throw new UnsupportedOperationException(String.format("ScriptEngine for %s does not implement %s", language, Compilable.class.getName()));

        final Compilable compilable = (Compilable) scriptEngine;
        return compilable.compile(script);
    }

    /**
     * Compiles a script without executing it.
     *
     * @throws UnsupportedOperationException if the {@link ScriptEngine} implementation does not implement
     * the {@link javax.script.Compilable} interface.
     */
    public CompiledScript compile(final Reader script, final String language) throws ScriptException {
        if (!scriptEngines.containsKey(language))
            throw new IllegalArgumentException("Language [%s] not supported");

        awaitControlOp();
        final ScriptEngine scriptEngine = scriptEngines.get(language);
        if (scriptEngine instanceof Compilable)
            throw new UnsupportedOperationException(String.format("ScriptEngine for %s does not implement %s", language, Compilable.class.getName()));

        final Compilable compilable = (Compilable) scriptEngine;
        return compilable.compile(script);
    }

    /**
     * Reload a {@code ScriptEngine} with fresh imports.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void reload(final String language, final Set<String> imports, final Set<String> staticImports,
                       final Map<String, Object> config) {
        try {
            signalControlOpStart();
            if (scriptEngines.containsKey(language))
                scriptEngines.remove(language);

            final ScriptEngine scriptEngine = createScriptEngine(language, imports, staticImports, config)
                    .orElseThrow(() -> new IllegalArgumentException("Language [%s] not supported"));
            scriptEngines.put(language, scriptEngine);

            logger.info("Loaded {} ScriptEngine", language);
        } finally {
            signalControlOpEnd();
        }
    }

    /**
     * Perform append to the existing import list for all {@code ScriptEngine} instances that implement the
     * {@link DependencyManager} interface.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void addImports(final Set<String> imports) {
        try {
            signalControlOpStart();
            getDependencyManagers().forEach(dm -> dm.addImports(imports));
        } finally {
            signalControlOpEnd();
        }
    }

    /**
     * Pull in dependencies given some Maven coordinates.  Cycle through each {@code ScriptEngine} and determine if it
     * implements {@link DependencyManager}.  For those that do call the @{link DependencyManager#use} method to fire
     * it up.  Waits for any existing script evaluations to complete but then blocks other operations until complete.
     */
    public List<GremlinPlugin> use(final String group, final String artifact, final String version) {
        final List<GremlinPlugin> pluginsToLoad = new ArrayList<>();
        try {
            signalControlOpStart();
            getDependencyManagers().forEach(dm -> {
                try {
                    pluginsToLoad.addAll(dm.use(group, artifact, version));
                } catch (Exception ex) {
                    logger.warn("Could not get dependency for [{}, {}, {}] - {}", group, artifact, version, ex.getMessage());
                }
            });
        } finally {
            signalControlOpEnd();
        }

        return pluginsToLoad;
    }

    /**
     * For each {@link DependencyManager} try to load the specified plugins.
     */
    public void loadPlugins(final List<GremlinPlugin> plugins) {
        try {
            signalControlOpStart();
            getDependencyManagers().forEach(dm -> {
                try {
                    dm.loadPlugins(plugins);
                } catch (IllegalEnvironmentException iee) {
                    logger.warn("Some plugins may not have been loaded to {} - {}", dm.getClass().getSimpleName(), iee.getMessage());
                } catch (Exception ex) {
                    logger.error(String.format("Some plugins may not have been loaded to %s", dm.getClass().getSimpleName()), ex);
                }
            });
        } finally {
            signalControlOpEnd();
        }
    }

    /**
     * Iterate through all the {@link ScriptEngine} implementations and if they implement {@link AutoCloseable}
     * then call the {@link AutoCloseable#close()} method. After that is complete, the script engine cache will be
     * cleared.
     */
    @Override
    public void close() throws Exception {
        try {
            signalControlOpStart();
            scriptEngines.values().stream()
                    .filter(se -> se instanceof AutoCloseable)
                    .map(se -> (AutoCloseable) se).forEach(c -> {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            });
            scriptEngines.clear();
        } finally {
            signalControlOpEnd();
        }
    }

    /**
     * Resets the ScriptEngines and re-initializes them.  Waits for any existing script evaluations to complete but
     * then blocks other operations until complete.
     */
    public void reset() {
        try {
            signalControlOpStart();
            getDependencyManagers().forEach(DependencyManager::reset);
        } finally {
            signalControlOpEnd();
            this.initializer.accept(this);
        }
    }

    /**
     * List dependencies for those {@code ScriptEngine} objects that implement the {@link DependencyManager} interface.
     */
    public Map<String, List<Map>> dependencies() {
        final Map<String, List<Map>> m = new HashMap<>();
        scriptEngines.entrySet().stream()
                .filter(kv -> kv.getValue() instanceof DependencyManager)
                .forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).dependencies())));
        return m;
    }

    /**
     * List the imports for those {@code ScriptEngine} objects that implement the {@link DependencyManager} interface.
     */
    public Map<String, List<Map>> imports() {
        final Map<String, List<Map>> m = new HashMap<>();
        scriptEngines.entrySet().stream()
                .filter(kv -> kv.getValue() instanceof DependencyManager)
                .forEach(kv -> m.put(kv.getKey(), Arrays.asList(((DependencyManager) kv.getValue()).imports())));
        return m;
    }

    /**
     * Get the set of {@code ScriptEngine} that implement {@link DependencyManager} interface.
     */
    private Set<DependencyManager> getDependencyManagers() {
        return scriptEngines.entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(se -> se instanceof DependencyManager)
                .map(se -> (DependencyManager) se)
                .collect(Collectors.<DependencyManager>toSet());
    }

    /**
     * Called when a control operation starts.  Attempts to grab a lock on the "control" process and parks the
     * current thread if it cannot.  Parked threads are queued until released by {@link #signalControlOpEnd()}.
     */
    private void signalControlOpStart() {
        boolean wasInterrupted = false;
        final Thread current = Thread.currentThread();
        controlWaiters.add(current);

        // Block while not first in queue or cannot acquire lock
        while (controlWaiters.peek() != current || !controlOperationExecuting.compareAndSet(false, true)) {
            LockSupport.park(this);
            if (Thread.interrupted())
                wasInterrupted = true;
        }

        controlWaiters.remove();
        if (wasInterrupted)
            current.interrupt();
    }

    /**
     * Called when a control operation is finished.  Releases the lock for the next thread trying to call a
     * control operation.  If there are no additional threads trying to call control operations then unpark
     * any evaluation threads that are waiting.
     */
    private void signalControlOpEnd() {
        controlOperationExecuting.set(false);
        LockSupport.unpark(controlWaiters.peek());

        // let the eval threads proceed as long as the control functions waiting are all complete
        if (controlWaiters.size() == 0) {
            Thread t = evalWaiters.poll();
            while (t != null) {
                LockSupport.unpark(t);
                t = evalWaiters.poll();
            }
        }
    }

    /**
     * If a control operation is executing or there are some in the queue to be executed, then block the current
     * thread until that process completes.
     */
    private void awaitControlOp() {
        if(controlWaiters.size() > 0 || controlOperationExecuting.get()) {
            evalWaiters.add(Thread.currentThread());
            LockSupport.park(this);
        }
    }

    private static synchronized Optional<ScriptEngine> createScriptEngine(final String language,
                                                                          final Set<String> imports,
                                                                          final Set<String> staticImports,
                                                                          final Map<String, Object> config) {
        // gremlin-groovy gets special initialization for mapper imports and such.  could implement this more
        // generically with the DependencyManager interface, but going to wait to see how other ScriptEngines
        // develop for TinkerPop3 before committing too deeply here to any specific way of doing this.
        if (language.equals(gremlinGroovyScriptEngineFactory.getLanguageName())) {
            final List<CompilerCustomizerProvider> providers = new ArrayList<>();
            providers.add(new DefaultImportCustomizerProvider(imports, staticImports));

            // the key to the config of the compilerCustomizerProvider is the fully qualified classname of a
            // CompilerCustomizerProvider.  the value is a list of arguments to pass to an available constructor.
            // the arguments must match in terms of type, so given that configuration typically comes from yaml
            // or properties file, it is best to stick to primitive values when possible here for simplicity.
            final Map<String,Object> compilerCustomizerProviders = (Map<String,Object>) config.getOrDefault(
                    "compilerCustomizerProviders", Collections.emptyMap());
            compilerCustomizerProviders.forEach((k,v) -> {
                try {
                    final Class providerClass = Class.forName(k);
                    if (v != null && v instanceof List && ((List) v).size() > 0) {
                        final List<Object> l = (List) v;
                        final Object[] args = new Object[l.size()];
                        l.toArray(args);

                        final Class<?>[] argClasses = new Class<?>[args.length];
                        Stream.of(args).map(a -> a.getClass()).collect(Collectors.toList()).toArray(argClasses);
                        final Constructor constructor = providerClass.getConstructor(argClasses);
                        providers.add((CompilerCustomizerProvider) constructor.newInstance(args));
                    } else {
                        providers.add((CompilerCustomizerProvider) providerClass.newInstance());
                    }
                } catch(Exception ex) {
                    logger.warn("Could not instantiate CompilerCustomizerProvider implementation [{}].  It will not be applied.", k);
                }
            });

            final CompilerCustomizerProvider[] providerArray = new CompilerCustomizerProvider[providers.size()];
            return Optional.of((ScriptEngine) new GremlinGroovyScriptEngine(providers.toArray(providerArray)));
        } else {
            return Optional.ofNullable(SCRIPT_ENGINE_MANAGER.getEngineByName(language));
        }
    }

    /**
     * Takes the bindings from a request for eval and merges them with the {@code ENGINE_SCOPE} bindings.
     */
    private static Bindings mergeBindings(final Bindings bindings, final ScriptEngine engine) {
        // plugins place "globals" here - see ScriptEnginePluginAcceptor
        final Bindings all = engine.getBindings(ScriptContext.ENGINE_SCOPE);

        // merge the globals with the incoming bindings where local bindings "win"
        all.putAll(bindings);
        return all;
    }
}
