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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Execute Gremlin scripts against a {@code ScriptEngine} instance.  It is designed to host any JSR-223 enabled
 * {@code ScriptEngine} and assumes such engines are designed to be thread-safe in the evaluation.  Script evaluation
 * functions return a {@link CompletableFuture} where scripts may timeout if their evaluation
 * takes too long.  The default timeout is 8000ms.
 * <p/>
 * By default, the {@code GremlinExecutor} initializes itself to use a shared thread pool initialized with four
 * threads. This default thread pool is shared for both the task of executing script evaluations and for scheduling
 * timeouts. It is worth noting that a timeout simply triggers the returned {@link CompletableFuture} to abort, but
 * the thread processing the script will continue to evaluate until completion.  This offers only marginal protection
 * against run-away scripts.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(GremlinExecutor.class);

    /**
     * {@link ScriptEngines} instance to evaluate Gremlin script requests.
     */
    private ScriptEngines scriptEngines;

    private final Map<String, EngineSettings> settings;
    private final long scriptEvaluationTimeout;
    private final Bindings globalBindings;
    private final List<List<String>> use;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Consumer<Bindings> beforeEval;
    private final Consumer<Bindings> afterSuccess;
    private final Consumer<Bindings> afterTimeout;
    private final BiConsumer<Bindings, Throwable> afterFailure;
    private final Set<String> enabledPlugins;
    private final boolean suppliedExecutor;
    private final boolean suppliedScheduledExecutor;

    private GremlinExecutor(final Builder builder, final boolean suppliedExecutor,
                            final boolean suppliedScheduledExecutor) {

        this.executorService = builder.executorService;
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.beforeEval = builder.beforeEval;
        this.afterSuccess = builder.afterSuccess;
        this.afterTimeout = builder.afterTimeout;
        this.afterFailure = builder.afterFailure;
        this.use = builder.use;
        this.settings = builder.settings;
        this.scriptEvaluationTimeout = builder.scriptEvaluationTimeout;
        this.globalBindings = builder.globalBindings;
        this.enabledPlugins = builder.enabledPlugins;
        this.scriptEngines = createScriptEngines();
        this.suppliedExecutor = suppliedExecutor;
        this.suppliedScheduledExecutor = suppliedScheduledExecutor;
    }

    /**
     * Attempts to compile a script and cache it in the default {@link javax.script.ScriptEngine}.  This is only
     * possible if the {@link javax.script.ScriptEngine} implementation implements {@link javax.script.Compilable}.
     * In the event that the default {@link javax.script.ScriptEngine} does not implement it, the method will
     * return empty.
     */
    public Optional<CompiledScript> compile(final String script) throws ScriptException {
        return compile(script, Optional.empty());
    }

    /**
     * Attempts to compile a script and cache it in the request {@link javax.script.ScriptEngine}.  This is only
     * possible if the {@link javax.script.ScriptEngine} implementation implements {@link javax.script.Compilable}.
     * In the event that the requested {@link javax.script.ScriptEngine} does not implement it, the method will
     * return empty.
     */
    public Optional<CompiledScript> compile(final String script, final Optional<String> language) throws ScriptException {
        final String lang = language.orElse("gremlin-groovy");
        try {
            return Optional.of(scriptEngines.compile(script, lang));
        } catch (UnsupportedOperationException uoe) {
            return Optional.empty();
        }
    }

    /**
     * Evaluate a script with empty bindings.
     */
    public CompletableFuture<Object> eval(final String script) {
        return eval(script, null, new SimpleBindings());
    }

    /**
     * Evaluate a script with specified bindings.
     */
    public CompletableFuture<Object> eval(final String script, final Bindings boundVars) {
        return eval(script, null, boundVars);
    }

    /**
     * Evaluate a script with a {@link Map} of bindings.
     */
    public CompletableFuture<Object> eval(final String script, final Map<String, Object> boundVars) {
        return eval(script, null, new SimpleBindings(boundVars));
    }

    /**
     * Evaluate a script.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings as a {@link Map} to evaluate in the context of the script
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Map<String, Object> boundVars) {
        return eval(script, language, new SimpleBindings(boundVars));
    }

    /**
     * Evaluate a script.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Bindings boundVars) {
        return eval(script, language, boundVars, null, null);
    }

    /**
     * Evaluate a script and allow for the submission of a transform {@link Function} that will transform the
     * result after script evaluates but before transaction commit and before the returned {@link CompletableFuture}
     * is completed.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Map<String, Object> boundVars,
                                          final Function<Object, Object> transformResult) {
        return eval(script, language, new SimpleBindings(boundVars), transformResult, null);
    }

    /**
     * Evaluate a script and allow for the submission of a {@link Consumer} that will take the result for additional
     * processing after the script evaluates and after the {@link CompletableFuture} is completed, but before the
     * transaction is committed.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param withResult a {@link Consumer} that accepts the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Map<String, Object> boundVars,
                                          final Consumer<Object> withResult) {
        return eval(script, language, new SimpleBindings(boundVars), null, withResult);
    }

    /**
     * Evaluate a script and allow for the submission of both a transform {@link Function} and {@link Consumer}.
     * The {@link Function} will transform the result after script evaluates but before transaction commit and before
     * the returned {@link CompletableFuture} is completed. The {@link Consumer} will take the result for additional
     * processing after the script evaluates and after the {@link CompletableFuture} is completed, but before the
     * transaction is committed.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     * @param withResult a {@link Consumer} that accepts the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Bindings boundVars,
                                          final Function<Object, Object> transformResult, final Consumer<Object> withResult) {
        final LifeCycle lifeCycle = LifeCycle.build()
                .transformResult(transformResult)
                .withResult(withResult).create();

        return eval(script, language, boundVars, lifeCycle);
    }

    /**
     * Evaluate a script and allow for the submission of alteration to the entire evaluation execution lifecycle.
     *
     * @param script the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param lifeCycle a set of functions that can be applied at various stages of the evaluation process
     */
    public CompletableFuture<Object> eval(final String script, final String language, final Bindings boundVars,  final LifeCycle lifeCycle) {
        final String lang = Optional.ofNullable(language).orElse("gremlin-groovy");

        logger.debug("Preparing to evaluate script - {} - in thread [{}]", script, Thread.currentThread().getName());

        final Bindings bindings = new SimpleBindings();
        bindings.putAll(globalBindings);
        bindings.putAll(boundVars);

        final CompletableFuture<Object> evaluationFuture = new CompletableFuture<>();
        final FutureTask<Void> f = new FutureTask<>(() -> {
            try {
                lifeCycle.getBeforeEval().orElse(beforeEval).accept(bindings);

                logger.debug("Evaluating script - {} - in thread [{}]", script, Thread.currentThread().getName());

                final Object o = scriptEngines.eval(script, bindings, lang);

                // apply a transformation before sending back the result - useful when trying to force serialization
                // in the same thread that the eval took place given ThreadLocal nature of graphs as well as some
                // transactional constraints
                final Object result = lifeCycle.getTransformResult().isPresent() ?
                        lifeCycle.getTransformResult().get().apply(o) : o;
                evaluationFuture.complete(result);

                // a mechanism for taking the final result and doing something with it in the same thread, but
                // AFTER the eval and transform are done and that future completed.  this provides a final means
                // for working with the result in the same thread as it was eval'd
                if (lifeCycle.getWithResult().isPresent()) lifeCycle.getWithResult().get().accept(result);

                lifeCycle.getAfterSuccess().orElse(afterSuccess).accept(bindings);
            } catch (Throwable ex) {
                final Throwable root = null == ex.getCause() ? ex : ExceptionUtils.getRootCause(ex);

                // thread interruptions will typically come as the result of a timeout, so in those cases,
                // check for that situation and convert to TimeoutException
                if (root instanceof InterruptedException)
                    evaluationFuture.completeExceptionally(new TimeoutException(
                            String.format("Script evaluation exceeded the configured threshold of %s ms for request [%s]: %s", scriptEvaluationTimeout, script, root.getMessage())));
                else {
                    lifeCycle.getAfterFailure().orElse(afterFailure).accept(bindings, root);
                    evaluationFuture.completeExceptionally(root);
                }
            }

            return null;
        });

        executorService.execute(f);

        if (scriptEvaluationTimeout > 0) {
            // Schedule a timeout in the thread pool for future execution
            final ScheduledFuture<?> sf = scheduledExecutorService.schedule(() -> {
                logger.warn("Timing out script - {} - in thread [{}]", script, Thread.currentThread().getName());
                if (!f.isDone()) {
                    lifeCycle.getAfterTimeout().orElse(afterTimeout).accept(bindings);
                    f.cancel(true);
                }
            }, scriptEvaluationTimeout, TimeUnit.MILLISECONDS);

            // Cancel the scheduled timeout if the eval future is complete or the script evaluation failed
            // with exception
            evaluationFuture.handleAsync((v, t) -> {
                logger.debug("Killing scheduled timeout on script evaluation as the eval completed (possibly with exception).");
                return sf.cancel(true);
            });
        }

        return evaluationFuture;
    }

    public ScriptEngines getScriptEngines() {
        return this.scriptEngines;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public Bindings getGlobalBindings() {
        return globalBindings;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Executors are only closed if they were not supplied externally in the
     * {@link org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.Builder}
     */
    @Override
    public void close() throws Exception {
        closeAsync().join();
    }

    /**
     * Executors are only closed if they were not supplied externally in the
     * {@link org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.Builder}
     */
    public CompletableFuture<Void> closeAsync() throws Exception {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        new Thread(() -> {
            // leave pools running if they are supplied externally.  let the sender be responsible for shutting them down
            if (!suppliedExecutor) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
                        logger.warn("Timeout while waiting for ExecutorService of GremlinExecutor to shutdown.");
                } catch (InterruptedException ie) {
                    logger.warn("ExecutorService on GremlinExecutor may not have shutdown properly as shutdown thread terminated early.");
                }
            }

            // calls to shutdown are idempotent so no problems calling it twice if the pool is shared
            if (!suppliedScheduledExecutor) {
                scheduledExecutorService.shutdown();
                try {
                    if (!scheduledExecutorService.awaitTermination(180000, TimeUnit.MILLISECONDS))
                        logger.warn("Timeout while waiting for ScheduledExecutorService of GremlinExecutor to shutdown.");
                } catch (InterruptedException ie) {
                    logger.warn("ScheduledExecutorService on GremlinExecutor may not have shutdown properly as shutdown thread terminated early.");
                }
            }

            try {
                scriptEngines.close();
            } catch (Exception ex) {
                logger.warn("Error while shutting down the ScriptEngines in the GremlinExecutor", ex);
            }

            future.complete(null);
        }, "gremlin-executor-close").start();

        return future;
    }

    private ScriptEngines createScriptEngines() {
        // plugins already on the path - ones static to the classpath
        final List<GremlinPlugin> globalPlugins = new ArrayList<>();
        ServiceLoader.load(GremlinPlugin.class).forEach(globalPlugins::add);

        return new ScriptEngines(se -> {
            // this first part initializes the scriptengines Map
            for (Map.Entry<String, EngineSettings> config : settings.entrySet()) {
                final String language = config.getKey();
                se.reload(language, new HashSet<>(config.getValue().getImports()),
                        new HashSet<>(config.getValue().getStaticImports()), config.getValue().getConfig());
            }

            // use grabs dependencies and returns plugins to load
            final List<GremlinPlugin> pluginsToLoad = new ArrayList<>(globalPlugins);
            use.forEach(u -> {
                if (u.size() != 3)
                    logger.warn("Could not resolve dependencies for [{}].  Each entry for the 'use' configuration must include [groupId, artifactId, version]", u);
                else {
                    logger.info("Getting dependencies for [{}]", u);
                    pluginsToLoad.addAll(se.use(u.get(0), u.get(1), u.get(2)));
                }
            });

            // now that all dependencies are in place, the imports can't get messed up if a plugin tries to execute
            // a script (as the script engine appends the import list to the top of all scripts passed to the engine).
            // only enable those plugins that are configured to be enabled.
            se.loadPlugins(pluginsToLoad.stream().filter(plugin -> enabledPlugins.contains(plugin.getName())).collect(Collectors.toList()));

            // initialization script eval can now be performed now that dependencies are present with "use"
            for (Map.Entry<String, EngineSettings> config : settings.entrySet()) {
                final String language = config.getKey();

                // script engine initialization files that fail will only log warnings - not fail server initialization
                final AtomicBoolean hasErrors = new AtomicBoolean(false);
                config.getValue().getScripts().stream().map(File::new).filter(f -> {
                    if (!f.exists()) {
                        logger.warn("Could not initialize {} ScriptEngine with {} as file does not exist", language, f);
                        hasErrors.set(true);
                    }

                    return f.exists();
                }).map(f -> {
                    try {
                        return Pair.with(f, Optional.of(new FileReader(f)));
                    } catch (IOException ioe) {
                        logger.warn("Could not initialize {} ScriptEngine with {} as file could not be read - {}", language, f, ioe.getMessage());
                        hasErrors.set(true);
                        return Pair.with(f, Optional.<FileReader>empty());
                    }
                }).filter(p -> p.getValue1().isPresent()).map(p -> Pair.with(p.getValue0(), p.getValue1().get())).forEachOrdered(p -> {
                    try {
                        final Bindings bindings = new SimpleBindings();
                        bindings.putAll(globalBindings);

                        // evaluate init scripts with hard reference so as to ensure it doesn't get garbage collected
                        bindings.put(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE, GremlinGroovyScriptEngine.REFERENCE_TYPE_HARD);

                        // the returned object should be a Map of initialized global bindings
                        final Object initializedBindings = se.eval(p.getValue1(), bindings, language);
                        if (initializedBindings != null && initializedBindings instanceof Map)
                            globalBindings.putAll((Map) initializedBindings);
                        else
                            logger.warn("Initialization script {} did not return a Map - no global bindings specified", p.getValue0());

                        logger.info("Initialized {} ScriptEngine with {}", language, p.getValue0());
                    } catch (ScriptException sx) {
                        hasErrors.set(true);
                        logger.warn("Could not initialize {} ScriptEngine with {} as script could not be evaluated - {}", language, p.getValue0(), sx.getMessage());
                    }
                });
            }
        });
    }

    /**
     * Create a {@code Builder} with the gremlin-groovy ScriptEngine configured.
     */
    public static Builder build() {
        return new Builder().addEngineSettings("gremlin-groovy", new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new HashMap<>());
    }

    /**
     * Create a {@code Builder} and specify the first ScriptEngine to be included.
     */
    public static Builder build(final String engineName, final List<String> imports,
                                final List<String> staticImports, final List<String> scripts,
                                final Map<String, Object> config) {
        return new Builder().addEngineSettings(engineName, imports, staticImports, scripts, config);
    }

    public final static class Builder {
        private long scriptEvaluationTimeout = 8000;
        private Map<String, EngineSettings> settings = new HashMap<>();
        private ExecutorService executorService = null;
        private ScheduledExecutorService scheduledExecutorService = null;
        private Set<String> enabledPlugins = new HashSet<>();
        private Consumer<Bindings> beforeEval = (b) -> {
        };
        private Consumer<Bindings> afterSuccess = (b) -> {
        };
        private Consumer<Bindings> afterTimeout = (b) -> {
        };
        private BiConsumer<Bindings, Throwable> afterFailure = (b, e) -> {
        };
        private List<List<String>> use = new ArrayList<>();
        private Bindings globalBindings = new SimpleBindings();

        private Builder() {
        }

        /**
         * Add a particular script engine for the executor to instantiate.
         *
         * @param engineName    The name of the engine as defined by the engine itself.
         * @param imports       A list of imports for the engine.
         * @param staticImports A list of static imports for the engine.
         * @param scripts       A list of scripts to execute in the engine to initialize it.
         * @param config        Custom configuration map for the ScriptEngine
         */
        public Builder addEngineSettings(final String engineName, final List<String> imports,
                                         final List<String> staticImports, final List<String> scripts,
                                         final Map<String, Object> config) {
            if (null == imports) throw new IllegalArgumentException("imports cannot be null");
            if (null == staticImports) throw new IllegalArgumentException("staticImports cannot be null");
            if (null == scripts) throw new IllegalArgumentException("scripts cannot be null");
            final Map<String, Object> m = null == config ? Collections.emptyMap() : config;

            settings.put(engineName, new EngineSettings(imports, staticImports, scripts, m));
            return this;
        }

        /**
         * Bindings to apply to every script evaluated.
         */
        public Builder globalBindings(final Bindings bindings) {
            this.globalBindings = bindings;
            return this;
        }

        /**
         * Amount of time a script has before it times out. Note that the time required covers both script evaluation
         * as well as any time needed for a post result transformation (if the transformation function is supplied
         * to the {@link GremlinExecutor#eval}).
         *
         * @param scriptEvaluationTimeout Time in milliseconds that a script is allowed to run and its
         *                                results potentially transformed. Set to zero to have no timeout set.
         */
        public Builder scriptEvaluationTimeout(final long scriptEvaluationTimeout) {
            this.scriptEvaluationTimeout = scriptEvaluationTimeout;
            return this;
        }

        /**
         * Replaces any settings provided by {@link #engineSettings(java.util.Map)}.
         */
        public Builder engineSettings(final Map<String, EngineSettings> settings) {
            this.settings = settings;
            return this;
        }

        /**
         * The thread pool used to evaluate scripts.
         */
        public Builder executorService(final ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        /**
         * The thread pool used to schedule timeouts on scripts.
         */
        public Builder scheduledExecutorService(final ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        /**
         * A {@link Consumer} to execute just before the script evaluation.
         */
        public Builder beforeEval(final Consumer<Bindings> beforeEval) {
            this.beforeEval = beforeEval;
            return this;
        }

        /**
         * A {@link Consumer} to execute just after successful script evaluation. Note that success will be called
         * after evaluation of the script in the engine and after the results have passed through transformation
         * (if a transform function is passed to the {@link GremlinExecutor#eval}.
         */
        public Builder afterSuccess(final Consumer<Bindings> afterSuccess) {
            this.afterSuccess = afterSuccess;
            return this;
        }

        /**
         * A {@link Consumer} to execute if the script times out.
         */
        public Builder afterTimeout(final Consumer<Bindings> afterTimeout) {
            this.afterTimeout = afterTimeout;
            return this;
        }

        /**
         * A {@link Consumer} to execute in the event of failure.
         */
        public Builder afterFailure(final BiConsumer<Bindings, Throwable> afterFailure) {
            this.afterFailure = afterFailure;
            return this;
        }

        /**
         * A set of maven coordinates for dependencies to be applied for the script engine instances.
         */
        public Builder use(final List<List<String>> use) {
            this.use = use;
            return this;
        }

        /**
         * Set of the names of plugins that should be enabled for the engine.
         */
        public Builder enabledPlugins(final Set<String> enabledPlugins) {
            this.enabledPlugins = enabledPlugins;
            return this;
        }

        public GremlinExecutor create() {
            final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("gremlin-executor-default-%d").build();

            final AtomicBoolean poolCreatedByBuilder = new AtomicBoolean();
            final AtomicBoolean suppliedExecutor = new AtomicBoolean(true);
            final AtomicBoolean suppliedScheduledExecutor = new AtomicBoolean(true);

            final ExecutorService es = Optional.ofNullable(executorService).orElseGet(() -> {
                poolCreatedByBuilder.set(true);
                suppliedExecutor.set(false);
                return Executors.newScheduledThreadPool(4, threadFactory);
            });
            executorService = es;

            final ScheduledExecutorService ses = Optional.ofNullable(scheduledExecutorService).orElseGet(() -> {
                // if the pool is created by the builder and we need another just re-use it, otherwise create
                // a new one of those guys
                suppliedScheduledExecutor.set(false);
                return (poolCreatedByBuilder.get()) ?
                        (ScheduledExecutorService) es : Executors.newScheduledThreadPool(4, threadFactory);
            });
            scheduledExecutorService = ses;

            return new GremlinExecutor(this, suppliedExecutor.get(), suppliedScheduledExecutor.get());
        }
    }

    private static class EngineSettings {
        private List<String> imports;
        private List<String> staticImports;
        private List<String> scripts;
        private Map<String, Object> config;

        public EngineSettings(final List<String> imports, final List<String> staticImports,
                              final List<String> scripts, final Map<String, Object> config) {
            this.imports = imports;
            this.staticImports = staticImports;
            this.scripts = scripts;
            this.config = config;
        }

        private List<String> getImports() {
            return imports;
        }

        private List<String> getStaticImports() {
            return staticImports;
        }

        private List<String> getScripts() {
            return scripts;
        }

        public Map<String, Object> getConfig() {
            return config;
        }
    }

    /**
     * The lifecycle of execution within the {@link #eval(String, String, Bindings, LifeCycle)} method. Since scripts
     * are executed in a thread pool and graph transactions are bound to a thread all actions related to that script
     * evaluation, both before and after that evaluation, need to be executed in the same thread.  This leads to a
     * lifecycle of actions that can occur within that evaluation.  Note that some of these options can be globally
     * set on the {@code GremlinExecutor} itself through the {@link GremlinExecutor.Builder}.  If specified here,
     * they will override those global settings.
     */
    public static class LifeCycle {
        private final Optional<Consumer<Bindings>> beforeEval;
        private final Optional<Function<Object, Object>> transformResult;
        private final Optional<Consumer<Object>> withResult;
        private final Optional<Consumer<Bindings>> afterSuccess;
        private final Optional<Consumer<Bindings>> afterTimeout;
        private final Optional<BiConsumer<Bindings, Throwable>> afterFailure;

        private LifeCycle(final Builder builder) {
            beforeEval = Optional.ofNullable(builder.beforeEval);
            transformResult = Optional.ofNullable(builder.transformResult);
            withResult = Optional.ofNullable(builder.withResult);
            afterSuccess = Optional.ofNullable(builder.afterSuccess);
            afterTimeout = Optional.ofNullable(builder.afterTimeout);
            afterFailure = Optional.ofNullable(builder.afterFailure);
        }

        public Optional<Consumer<Bindings>> getBeforeEval() {
            return beforeEval;
        }

        public Optional<Function<Object, Object>> getTransformResult() {
            return transformResult;
        }

        public Optional<Consumer<Object>> getWithResult() {
            return withResult;
        }

        public Optional<Consumer<Bindings>> getAfterSuccess() {
            return afterSuccess;
        }

        public Optional<Consumer<Bindings>> getAfterTimeout() {
            return afterTimeout;
        }

        public Optional<BiConsumer<Bindings, Throwable>> getAfterFailure() {
            return afterFailure;
        }

        public static Builder build() {
            return new Builder();
        }

        public static class Builder {
            private Consumer<Bindings> beforeEval = null;
            private Function<Object, Object> transformResult = null;
            private Consumer<Object> withResult = null;
            private Consumer<Bindings> afterSuccess = null;
            private Consumer<Bindings> afterTimeout = null;
            private BiConsumer<Bindings, Throwable> afterFailure = null;

            /**
             * Specifies the function to execute prior to the script being evaluated.  This function can also be
             * specified globally on {@link GremlinExecutor.Builder#beforeEval(Consumer)}.
             */
            public Builder beforeEval(final Consumer<Bindings> beforeEval) {
                this.beforeEval = beforeEval;
                return this;
            }

            /**
             * Specifies the function to execute on the result of the script evaluation just after script evaluation
             * returns but before the script evaluation is marked as complete.
             */
            public Builder transformResult(final Function<Object, Object> transformResult) {
                this.transformResult = transformResult;
                return this;
            }

            /**
             * Specifies the function to execute on the result of the script evaluation just after script evaluation
             * returns but after the script evaluation is marked as complete.
             */
            public Builder withResult(final Consumer<Object> withResult) {
                this.withResult = withResult;
                return this;
            }

            /**
             * Specifies the function to execute after result transformations.  This function can also be
             * specified globally on {@link GremlinExecutor.Builder#afterSuccess(Consumer)}.
             */
            public Builder afterSuccess(final Consumer<Bindings> afterSuccess) {
                this.afterSuccess = afterSuccess;
                return this;
            }

            /**
             * Specifies the function to execute if the script evaluation times out.  This function can also be
             * specified globally on {@link GremlinExecutor.Builder#afterTimeout(Consumer)}.
             */
            public Builder afterTimeout(final Consumer<Bindings> afterTimeout) {
                this.afterTimeout = afterTimeout;
                return this;
            }

            /**
             * Specifies the function to execute if the script evaluation fails.  This function can also be
             * specified globally on {@link GremlinExecutor.Builder#afterFailure(BiConsumer)}.
             */
            public Builder afterFailure(final BiConsumer<Bindings, Throwable> afterFailure) {
                this.afterFailure = afterFailure;
                return this;
            }

            public LifeCycle create() {
                return new LifeCycle(this);
            }
        }
    }
}
