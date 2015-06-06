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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Graphs;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * The core of script execution in Gremlin Server.  Given {@link Settings} and optionally other arguments, this
 * class will construct a {@link GremlinExecutor} to be used by Gremlin Server.  Those expecting to build their
 * own version of Gremlin Server might consider coring their implementation with this class as it provides some
 * basic infrastructure required for most of Gremlin Server script processing features.  Those embedding Gremlin
 * Server in another application might consider using this class to initialize the {@link GremlinServer} class
 * as it will allow sharing of thread pool resources.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerGremlinExecutor<T extends ScheduledExecutorService> {
    private static final Logger logger = LoggerFactory.getLogger(ServerGremlinExecutor.class);

    private final Graphs graphs;
    private final Settings settings;
    private final List<LifeCycleHook> hooks;

    private final T scheduledExecutorService;
    private final ExecutorService gremlinExecutorService;
    private final GremlinExecutor gremlinExecutor;

    /**
     * Create a new object from {@link Settings} where thread pools are internally created. Note that the
     * {@code scheduleExecutorServiceClass} will be created via
     * {@link Executors#newScheduledThreadPool(int, ThreadFactory)}.
     */
    public ServerGremlinExecutor(final Settings settings, final Class<T> scheduleExecutorServiceClass) {
        this(settings, null, null, scheduleExecutorServiceClass);
    }

    /**
     * Create a new object from {@link Settings} where thread pools are internally created. Note that if the
     * {@code scheduleExecutorServiceClass} is set to {@code null} it will be created via
     * {@link Executors#newScheduledThreadPool(int, ThreadFactory)}.  If either of the {@link ExecutorService}
     * instances are supplied, the {@link Settings#gremlinPool} value will be ignored for the pool size.
     */
    public ServerGremlinExecutor(final Settings settings, final ExecutorService gremlinExecutorService,
                                 final T scheduledExecutorService, final Class<T> scheduleExecutorServiceClass) {
        this.settings = settings;

        if (null == gremlinExecutorService) {
            final ThreadFactory threadFactoryGremlin = ThreadFactoryUtil.create("exec-%d");
            this.gremlinExecutorService = Executors.newFixedThreadPool(settings.gremlinPool, threadFactoryGremlin);
        } else {
            this.gremlinExecutorService = gremlinExecutorService;
        }

        if (null == scheduledExecutorService) {
            final ThreadFactory threadFactoryGremlin = ThreadFactoryUtil.create("worker-%d");
            this.scheduledExecutorService = scheduleExecutorServiceClass.cast(
                    Executors.newScheduledThreadPool(settings.threadPoolWorker, threadFactoryGremlin));
        } else {
            this.scheduledExecutorService = scheduledExecutorService;
        }

        // initialize graphs from configuration
        graphs = new Graphs(settings);

        logger.info("Initialized Gremlin thread pool.  Threads in pool named with pattern gremlin-*");

        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .scriptEvaluationTimeout(settings.scriptEvaluationTimeout)
                .afterFailure((b, e) -> graphs.rollbackAll())
                .afterSuccess(b -> graphs.commitAll())
                .beforeEval(b -> graphs.rollbackAll())
                .afterTimeout(b -> graphs.rollbackAll())
                .enabledPlugins(new HashSet<>(settings.plugins))
                .globalBindings(graphs.getGraphsAsBindings())
                .promoteBindings(kv -> kv.getValue() instanceof Graph
                        || kv.getValue() instanceof TraversalSource
                        || kv.getValue() instanceof LifeCycleHook)
                .executorService(this.gremlinExecutorService)
                .scheduledExecutorService(this.scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // make sure that server related classes are available at init
            v.imports.add(LifeCycleHook.class.getCanonicalName());
            gremlinExecutorBuilder.addEngineSettings(k, v.imports, v.staticImports, v.scripts, v.config);
        });

        gremlinExecutor = gremlinExecutorBuilder.create();

        logger.info("Initialized GremlinExecutor and configured ScriptEngines.");

        // script engine init may have altered the graph bindings or maybe even created new ones - need to
        // re-apply those references back
        gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof Graph)
                .forEach(kv -> graphs.getGraphs().put(kv.getKey(), (Graph) kv.getValue()));

        // script engine init may have constructed the TraversalSource bindings - store them in Graphs object
        gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof TraversalSource)
                .forEach(kv -> {
                    logger.info("A {} is now bound to [{}] with {}", kv.getValue().getClass().getSimpleName(), kv.getKey(), kv.getValue());
                    graphs.getTraversalSources().put(kv.getKey(), (TraversalSource) kv.getValue());
                });

        // determine if the initialization scripts introduced LifeCycleHook objects - if so we need to gather them
        // up for execution
        hooks = gremlinExecutor.getGlobalBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof LifeCycleHook)
                .map(kv -> (LifeCycleHook) kv.getValue())
                .collect(Collectors.toList());
    }

    public T getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public ExecutorService getGremlinExecutorService() {
        return gremlinExecutorService;
    }

    public Graphs getGraphs() {
        return graphs;
    }

    public Settings getSettings() {
        return settings;
    }

    public List<LifeCycleHook> getHooks() {
        return hooks;
    }
}
