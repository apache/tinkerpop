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
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

/**
 * The core of script execution in Gremlin Server.  Given {@link Settings} and optionally other arguments, this
 * class will construct a {@link GremlinExecutor} to be used by Gremlin Server.  A typical usage would be to
 * instantiate the {@link GremlinServer} and then immediately call {@link GremlinServer#getServerGremlinExecutor()}
 * which would allow the opportunity to assign "host options" which could be used by a custom {@link Channelizer}.
 * Add these options before calling {@link GremlinServer#start()} to be sure the {@link Channelizer} gets access to
 * those.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerGremlinExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ServerGremlinExecutor.class);

    private final GraphManager graphManager;
    private final Settings settings;
    private final List<LifeCycleHook> hooks;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService gremlinExecutorService;
    private final GremlinExecutor gremlinExecutor;

    private final Map<String,Object> hostOptions = new ConcurrentHashMap<>();

    /**
     * Create a new object from {@link Settings} where thread pools are externally assigned. Note that if the
     * {@code scheduleExecutorServiceClass} is set to {@code null} it will be created via
     * {@link Executors#newScheduledThreadPool(int, ThreadFactory)}.  If either of the {@link ExecutorService}
     * instances are supplied, the {@link Settings#gremlinPool} value will be ignored for the pool size.
     */
    public ServerGremlinExecutor(final Settings settings, final ExecutorService gremlinExecutorService,
                                 final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;

        try {
            final Class<?> clazz = Class.forName(settings.graphManager);
            final Constructor c = clazz.getConstructor(Settings.class);
            graphManager = (GraphManager) c.newInstance(settings);
        } catch (ClassNotFoundException e) {
            logger.error("Could not find GraphManager implementation "
                         + "defined by the 'graphManager' setting as: {}",
                         settings.graphManager);
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Could not invoke constructor on class {} (defined by "
                         + "the 'graphManager' setting) with one argument of "
                         + "class Settings",
                         settings.graphManager);
            throw new RuntimeException(e);
        }

        if (null == gremlinExecutorService) {
            final ThreadFactory threadFactoryGremlin = ThreadFactoryUtil.create("exec-%d");
            this.gremlinExecutorService = Executors.newFixedThreadPool(settings.gremlinPool, threadFactoryGremlin);
        } else {
            this.gremlinExecutorService = gremlinExecutorService;
        }

        if (null == scheduledExecutorService) {
            final ThreadFactory threadFactoryGremlin = ThreadFactoryUtil.create("worker-%d");
            this.scheduledExecutorService = Executors.newScheduledThreadPool(settings.threadPoolWorker, threadFactoryGremlin);
        } else {
            this.scheduledExecutorService = scheduledExecutorService;
        }

        logger.info("Initialized Gremlin thread pool.  Threads in pool named with pattern gremlin-*");

        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .evaluationTimeout(settings.getEvaluationTimeout())
                .afterFailure((b, e) -> this.graphManager.rollbackAll())
                .beforeEval(b -> this.graphManager.rollbackAll())
                .afterTimeout(b -> this.graphManager.rollbackAll())
                .globalBindings(this.graphManager.getAsBindings())
                .executorService(this.gremlinExecutorService)
                .scheduledExecutorService(this.scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // use plugins if they are present
            if (!v.plugins.isEmpty()) {
                // make sure that server related classes are available at init - new approach. the LifeCycleHook stuff
                // will be added explicitly via configuration using GremlinServerGremlinModule in the yaml
                gremlinExecutorBuilder.addPlugins(k, v.plugins);
            }
        });

        gremlinExecutor = gremlinExecutorBuilder.create();

        logger.info("Initialized GremlinExecutor and preparing GremlinScriptEngines instances.");

        // force each scriptengine to process something so that the init scripts will fire (this is necessary if
        // the GremlinExecutor is using the GremlinScriptEngineManager. this is a bit of hack, but it at least allows
        // the global bindings to become available after the init scripts are run (DefaultGremlinScriptEngineManager
        // runs the init scripts when the GremlinScriptEngine is created.
        settings.scriptEngines.keySet().forEach(engineName -> {
            try {
                // use no timeout on the engine initialization - perhaps this can be a configuration later
                final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                        evaluationTimeoutOverride(0L).create();
                gremlinExecutor.eval("1+1", engineName, new SimpleBindings(Collections.emptyMap()), lifeCycle).join();
                registerMetrics(engineName);
                logger.info("Initialized {} GremlinScriptEngine and registered metrics", engineName);
            } catch (Exception ex) {
                logger.warn(String.format("Could not initialize %s GremlinScriptEngine as init script could not be evaluated", engineName), ex);
            }
        });

        // script engine init may have altered the graph bindings or maybe even created new ones - need to
        // re-apply those references back
        gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof Graph)
                .forEach(kv -> this.graphManager.putGraph(kv.getKey(), (Graph) kv.getValue()));

        // script engine init may have constructed the TraversalSource bindings - store them in Graphs object
        gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof TraversalSource)
                .forEach(kv -> {
                    logger.info("A {} is now bound to [{}] with {}", kv.getValue().getClass().getSimpleName(), kv.getKey(), kv.getValue());
                    this.graphManager.putTraversalSource(kv.getKey(), (TraversalSource) kv.getValue());
                });

        // determine if the initialization scripts introduced LifeCycleHook objects - if so we need to gather them
        // up for execution
        hooks = gremlinExecutor.getScriptEngineManager().getBindings().entrySet().stream()
                .filter(kv -> kv.getValue() instanceof LifeCycleHook)
                .map(kv -> (LifeCycleHook) kv.getValue())
                .collect(Collectors.toList());
    }

    private void registerMetrics(final String engineName) {
        final GremlinScriptEngine engine = gremlinExecutor.getScriptEngineManager().getEngineByName(engineName);
        MetricManager.INSTANCE.registerGremlinScriptEngineMetrics(engine, engineName, "sessionless", "class-cache");
    }

    public void addHostOption(final String key, final Object value) {
        hostOptions.put(key, value);
    }

    public Map<String,Object> getHostOptions() {
        return Collections.unmodifiableMap(hostOptions);
    }

    public Object removeHostOption(final String key) {
        return hostOptions.remove(key);
    }

    public void clearHostOptions() {
        hostOptions.clear();
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public ExecutorService getGremlinExecutorService() {
        return gremlinExecutorService;
    }

    public GraphManager getGraphManager() {
        return graphManager;
    }

    public Settings getSettings() {
        return settings;
    }

    public List<LifeCycleHook> getHooks() {
        return hooks;
    }
}
