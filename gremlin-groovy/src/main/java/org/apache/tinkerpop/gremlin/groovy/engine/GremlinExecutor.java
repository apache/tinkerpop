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

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.jsr223.CachedGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.ConcurrentBindings;
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.InterruptedIOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

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

    private GremlinScriptEngineManager gremlinScriptEngineManager;

    private final Map<String, Map<String, Map<String,Object>>> plugins;
    private final long evaluationTimeout;
    private final Bindings globalBindings;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Consumer<Bindings> beforeEval;
    private final Consumer<Bindings> afterSuccess;
    private final BiConsumer<Bindings, Throwable> afterTimeout;
    private final BiConsumer<Bindings, Throwable> afterFailure;
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
        this.plugins = builder.plugins;
        this.evaluationTimeout = builder.evaluationTimeout;
        this.globalBindings = builder.globalBindings;

        this.gremlinScriptEngineManager = new CachedGremlinScriptEngineManager();
        initializeGremlinScriptEngineManager();

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
     * possible if the {@link javax.script.ScriptEngine} implementation implements {@link Compilable}.
     * In the event that the requested {@link javax.script.ScriptEngine} does not implement it, the method will
     * return empty.
     */
    public Optional<CompiledScript> compile(final String script, final Optional<String> language) throws ScriptException {
        final String lang = language.orElse("gremlin-groovy");
        try {
            final GremlinScriptEngine scriptEngine = gremlinScriptEngineManager.getEngineByName(lang);
            if (scriptEngine instanceof Compilable)
                return Optional.of(((Compilable) scriptEngine).compile(script));
            else
                return Optional.empty();
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
        return eval(script, language, boundVars, null, null, null);
    }

    /**
     * Evaluate a script and allow for the submission of a transform {@link Function} that will transform the
     * result after script evaluates but before transaction commit and before the returned {@link CompletableFuture}
     * is completed.
     *
     * @param gremlin the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String gremlin, final String language, final Map<String, Object> boundVars,
                                          final Function<Object, Object> transformResult) {
        return eval(gremlin, language, new SimpleBindings(boundVars), transformResult, null);
    }

    /**
     * Evaluate a script and allow for the submission of a transform {@link Function} that will transform the
     * result after script evaluates but before transaction commit and before the returned {@link CompletableFuture}
     * is completed.
     *
     * @param gremlin the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param timeOut optional override for evaluation timeout
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String gremlin, final String language, final Map<String, Object> boundVars,
                                          final Long timeOut, final Function<Object, Object> transformResult) {
        return eval(gremlin, language, new SimpleBindings(boundVars), timeOut, transformResult, null);
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
     * @param gremlin the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     * @param withResult a {@link Consumer} that accepts the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String gremlin, final String language, final Bindings boundVars,
                                          final Function<Object, Object> transformResult, final Consumer<Object> withResult) {
        return eval(gremlin, language, boundVars, null, transformResult, withResult);
    }

    /**
     * Evaluate a script and allow for the submission of both a transform {@link Function} and {@link Consumer}.
     * The {@link Function} will transform the result after script evaluates but before transaction commit and before
     * the returned {@link CompletableFuture} is completed. The {@link Consumer} will take the result for additional
     * processing after the script evaluates and after the {@link CompletableFuture} is completed, but before the
     * transaction is committed.
     *
     * @param gremlin the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param timeOut optional override for evaluation timeout
     * @param transformResult a {@link Function} that transforms the result - can be {@code null}
     * @param withResult a {@link Consumer} that accepts the result - can be {@code null}
     */
    public CompletableFuture<Object> eval(final String gremlin, final String language, final Bindings boundVars, final Long timeOut,
                                          final Function<Object, Object> transformResult, final Consumer<Object> withResult) {
        final LifeCycle lifeCycle = LifeCycle.build()
                .evaluationTimeoutOverride(timeOut)
                .transformResult(transformResult)
                .withResult(withResult).create();

        return eval(gremlin, language, boundVars, lifeCycle);
    }

    /**
     * Evaluate a script and allow for the submission of alteration to the entire evaluation execution lifecycle.
     *
     * @param gremlin the script to evaluate
     * @param language the language to evaluate it in
     * @param boundVars the bindings to evaluate in the context of the script
     * @param lifeCycle a set of functions that can be applied at various stages of the evaluation process
     */
    public CompletableFuture<Object> eval(final String gremlin, final String language, final Bindings boundVars, final LifeCycle lifeCycle) {
        final String lang = Optional.ofNullable(language).orElse("gremlin-groovy");

        if (logger.isDebugEnabled()) {
            logger.debug("Preparing to evaluate script - {} - in thread [{}]", gremlin, Thread.currentThread().getName());
        }

        final Bindings bindings = new SimpleBindings();
        bindings.putAll(globalBindings);
        bindings.putAll(boundVars);

        // override the timeout if the lifecycle has a value assigned. if the script contains with(timeout)
        // options then allow that value to override what's provided on the lifecycle
        final Optional<Long> timeoutDefinedInScript = GremlinScriptChecker.parse(gremlin).getTimeout();
        final long scriptEvalTimeOut = timeoutDefinedInScript.orElse(
                lifeCycle.getEvaluationTimeoutOverride().orElse(evaluationTimeout));

        final CompletableFuture<Object> evaluationFuture = new CompletableFuture<>();
        final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            try {
                lifeCycle.getBeforeEval().orElse(beforeEval).accept(bindings);

                logger.debug("Evaluating script - {} - in thread [{}]", gremlin, Thread.currentThread().getName());

                final Object o =gremlinScriptEngineManager.getEngineByName(lang).eval(gremlin, bindings);

                // apply a transformation before sending back the result - useful when trying to force serialization
                // in the same thread that the eval took place given ThreadLocal nature of graphs as well as some
                // transactional constraints
                final Object result = lifeCycle.getTransformResult().isPresent() ?
                        lifeCycle.getTransformResult().get().apply(o) : o;

                // a mechanism for taking the final result and doing something with it in the same thread, but
                // AFTER the eval and transform are done and that future completed.  this provides a final means
                // for working with the result in the same thread as it was eval'd
                if (lifeCycle.getWithResult().isPresent()) lifeCycle.getWithResult().get().accept(result);

                lifeCycle.getAfterSuccess().orElse(afterSuccess).accept(bindings);

                // the evaluationFuture must be completed after all processing as an exception in lifecycle events
                // that must raise as an exception to the caller who has the returned evaluationFuture. in other words,
                // if it occurs before this point, then the handle() method won't be called again if there is an
                // exception that ends up below trying to completeExceptionally()
                evaluationFuture.complete(result);
            } catch (Throwable ex) {
                final Throwable root = null == ex.getCause() ? ex : ExceptionUtils.getRootCause(ex);

                // thread interruptions will typically come as the result of a timeout, so in those cases,
                // check for that situation and convert to TimeoutException
                if (root instanceof InterruptedException
                        || root instanceof TraversalInterruptedException
                        || root instanceof InterruptedIOException) {
                    lifeCycle.getAfterTimeout().orElse(afterTimeout).accept(bindings, root);
                    evaluationFuture.completeExceptionally(new TimeoutException(
                            String.format("Evaluation exceeded the configured 'evaluationTimeout' threshold of %s ms or evaluation was otherwise cancelled directly for request [%s]: %s", scriptEvalTimeOut, gremlin, root.getMessage())));
                } else {
                    lifeCycle.getAfterFailure().orElse(afterFailure).accept(bindings, root);
                    evaluationFuture.completeExceptionally(root);
                }
            }

            return null;
        });

        final WeakReference<CompletableFuture<Object>> evaluationFutureRef = new WeakReference<>(evaluationFuture);
        final Future<?> executionFuture = executorService.submit(evalFuture);
        if (scriptEvalTimeOut > 0) {
            // Schedule a timeout in the thread pool for future execution
            final ScheduledFuture<?> sf = scheduledExecutorService.schedule(() -> {
                if (executionFuture.cancel(true)) {
                    final CompletableFuture<Object> ef = evaluationFutureRef.get();
                    if (ef != null) {
                        ef.completeExceptionally(new TimeoutException(
                                String.format("Evaluation exceeded the configured 'evaluationTimeout' threshold of %s ms or evaluation was otherwise cancelled directly for request [%s]", scriptEvalTimeOut, gremlin)));
                    }
                }
            }, scriptEvalTimeOut, TimeUnit.MILLISECONDS);

            // Cancel the scheduled timeout if the eval future is complete or the script evaluation failed with exception
            evaluationFuture.handleAsync((v, t) -> {
                if (!sf.isDone()) {
                    logger.debug("Killing scheduled timeout on script evaluation - {} - as the eval completed (possibly with exception).", gremlin);
                    sf.cancel(true);
                }

                // no return is necessary - nothing downstream is concerned with what happens in here
                return null;
            }, scheduledExecutorService);
        }

        return evaluationFuture;
    }

    public GremlinScriptEngineManager getScriptEngineManager() {
        return this.gremlinScriptEngineManager;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Executors are only closed if they were not supplied externally in the {@link GremlinExecutor.Builder}
     */
    @Override
    public void close() throws Exception {
        closeAsync().join();
    }

    /**
     * Executors are only closed if they were not supplied externally in the {@link GremlinExecutor.Builder}
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

            future.complete(null);
        }, "gremlin-executor-close").start();

        return future;
    }

    private void initializeGremlinScriptEngineManager() {
        for (Map.Entry<String, Map<String, Map<String,Object>>> config : plugins.entrySet()) {
            final String language = config.getKey();
            final Map<String, Map<String,Object>> pluginConfigs = config.getValue();
            for (Map.Entry<String, Map<String,Object>> pluginConfig : pluginConfigs.entrySet()) {
                try {
                    final Class<?> clazz = Class.forName(pluginConfig.getKey());

                    // first try instance() and if that fails try to use build()
                    try {
                        final Method instanceMethod = clazz.getMethod("instance");
                        gremlinScriptEngineManager.addPlugin((GremlinPlugin) instanceMethod.invoke(null));
                    } catch (Exception ex) {
                        final Method builderMethod = clazz.getMethod("build");
                        Object pluginBuilder = builderMethod.invoke(null);

                        final Class<?> builderClazz = pluginBuilder.getClass();
                        final Map<String, Object> customizerConfigs = pluginConfig.getValue();
                        final Method[] methods = builderClazz.getMethods();
                        for (Map.Entry<String, Object> customizerConfig : customizerConfigs.entrySet()) {
                            final Method configMethod = Stream.of(methods).filter(m -> {
                                final Class<?> type = customizerConfig.getValue().getClass();
                                return m.getName().equals(customizerConfig.getKey()) && m.getParameters().length <= 1
                                        && ClassUtils.isAssignable(type, m.getParameters()[0].getType(), true);
                            }).findFirst()
                                    .orElseThrow(() -> new IllegalStateException("Could not find builder method '" + customizerConfig.getKey() + "' on " + builderClazz.getCanonicalName()));
                            if (null == customizerConfig.getValue())
                                pluginBuilder = configMethod.invoke(pluginBuilder);
                            else
                                pluginBuilder = configMethod.invoke(pluginBuilder, customizerConfig.getValue());
                        }

                        try {
                            final Method appliesTo = builderClazz.getMethod("appliesTo", Collection.class);
                            pluginBuilder = appliesTo.invoke(pluginBuilder, Collections.singletonList(language));
                        } catch (NoSuchMethodException ignored) {

                        }

                        final Method create = builderClazz.getMethod("create");
                        gremlinScriptEngineManager.addPlugin((GremlinPlugin) create.invoke(pluginBuilder));
                    }
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }

        gremlinScriptEngineManager.setBindings(globalBindings);
    }

    /**
     * Create a {@code Builder} with the gremlin-groovy ScriptEngine configured.
     */
    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {
        private long evaluationTimeout = 8000;

        private Map<String, Map<String, Map<String,Object>>> plugins = new HashMap<>();

        private ExecutorService executorService = null;
        private ScheduledExecutorService scheduledExecutorService = null;
        private Consumer<Bindings> beforeEval = (b) -> {
        };
        private Consumer<Bindings> afterSuccess = (b) -> {
        };
        private BiConsumer<Bindings, Throwable> afterTimeout = (b, e) -> {
        };
        private BiConsumer<Bindings, Throwable> afterFailure = (b, e) -> {
        };
        private Bindings globalBindings = new ConcurrentBindings();

        private Builder() {
        }

        /**
         * Add a configuration for a {@link GremlinPlugin} to the executor. The key is the fully qualified class name
         * of the {@link GremlinPlugin} instance and the value is a map of configuration values. In that map, the key
         * is the name of a builder method on the {@link GremlinPlugin} and the value is some argument to pass to that
         * method.
         */
        public Builder addPlugins(final String engineName, final Map<String, Map<String,Object>> plugins) {
            this.plugins.put(engineName, plugins);
            return this;
        }

        /**
         * Bindings to apply to every script evaluated. Note that the entries of the supplied {@code Bindings} object
         * will be copied into a newly created {@link ConcurrentBindings} object
         * at the call of this method.
         */
        public Builder globalBindings(final Bindings bindings) {
            this.globalBindings = new ConcurrentBindings(bindings);
            return this;
        }

        /**
         * Amount of time an evaluation has before it times out. Note that the time required covers both evaluation
         * as well as any time needed for a post result transformation (if the transformation function is supplied
         * to the {@link GremlinExecutor#eval}).
         *
         * @param evaluationTimeout Time in milliseconds that an evaluation is allowed to run and its
         *                          results potentially transformed. Set to zero to have no timeout set.
         */
        public Builder evaluationTimeout(final long evaluationTimeout) {
            this.evaluationTimeout = evaluationTimeout;
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
         * @deprecated As of release 3.6.2, replaced by {@link #afterTimeout(BiConsumer)}.
         */
        @Deprecated
        public Builder afterTimeout(final Consumer<Bindings> afterTimeout) {
          BiConsumer<Bindings, Throwable> updatedAfterTimeout = (b, t) -> {
            afterTimeout.accept(b);
          };
          return afterTimeout(updatedAfterTimeout);
        }

        /**
         * A {@link BiConsumer} to execute if the script times out.
         */
        public Builder afterTimeout(final BiConsumer<Bindings, Throwable> afterTimeout) {
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
        private final Optional<BiConsumer<Bindings, Throwable>> afterTimeout;
        private final Optional<BiConsumer<Bindings, Throwable>> afterFailure;
        private final Optional<Long> evaluationTimeoutOverride;

        private LifeCycle(final Builder builder) {
            beforeEval = Optional.ofNullable(builder.beforeEval);
            transformResult = Optional.ofNullable(builder.transformResult);
            withResult = Optional.ofNullable(builder.withResult);
            afterSuccess = Optional.ofNullable(builder.afterSuccess);
            afterTimeout = Optional.ofNullable(builder.afterTimeout);
            afterFailure = Optional.ofNullable(builder.afterFailure);
            evaluationTimeoutOverride = Optional.ofNullable(builder.evaluationTimeoutOverride);
        }

        public Optional<Long> getEvaluationTimeoutOverride() {
            return evaluationTimeoutOverride;
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

        public Optional<BiConsumer<Bindings, Throwable>> getAfterTimeout() {
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
            private BiConsumer<Bindings, Throwable> afterTimeout = null;
            private BiConsumer<Bindings, Throwable> afterFailure = null;
            private Long evaluationTimeoutOverride = null;

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
             * returns but before the script evaluation is marked as complete.
             */
            public Builder withResult(final Consumer<Object> withResult) {
                this.withResult = withResult;
                return this;
            }

            /**
             * Specifies the function to execute after result transformations.  This function can also be
             * specified globally on {@link GremlinExecutor.Builder#afterSuccess(Consumer)}. The script evaluation
             * will be marked as "complete" after this method.
             */
            public Builder afterSuccess(final Consumer<Bindings> afterSuccess) {
                this.afterSuccess = afterSuccess;
                return this;
            }

            /**
             * Specifies the function to execute if the script evaluation times out. This
             * function can also be
             * specified globally on
             * {@link GremlinExecutor.Builder#afterTimeout(BiConsumer)}.
             */
            public Builder afterTimeout(final BiConsumer<Bindings, Throwable> afterTimeout) {
                this.afterTimeout = afterTimeout;
                return this;
            }

            /**
             * Specifies the function to execute if the script evaluation times out. This
             * function can also be
             * specified globally on
             * {@link GremlinExecutor.Builder#afterTimeout(BiConsumer)}.
             */
            public Builder afterTimeout(final Consumer<Bindings> afterTimeout) {
              this.afterTimeout = new BiConsumer<Bindings,Throwable>() {
                @Override
                public void accept(final Bindings arg0, final Throwable arg1) {
                  // TODO Auto-generated method stub
                  
                }
              };
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

            /**
             * An override to the global {@code evaluationTimeout} setting on the script engine. If this value
             * is set to {@code null} (the default) it will use the global setting.
             */
            public Builder evaluationTimeoutOverride(final Long evaluationTimeoutOverride) {
                this.evaluationTimeoutOverride = evaluationTimeoutOverride;
                return this;
            }

            public LifeCycle create() {
                return new LifeCycle(this);
            }
        }
    }
}
