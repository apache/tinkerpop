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
package org.apache.tinkerpop.gremlin.server.op.session;

import io.netty.channel.Channel;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.ThreadFactoryUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED;
import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.CONFIG_PER_GRAPH_CLOSE_TIMEOUT;
import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.CONFIG_SESSION_TIMEOUT;
import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.DEFAULT_PER_GRAPH_CLOSE_TIMEOUT;
import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.DEFAULT_SESSION_TIMEOUT;

/**
 * Defines a "session" for the {@link SessionOpProcessor} which preserves state between requests made to Gremlin
 * Server. Since transactions are bound to a single thread the "session" maintains its own thread to process Gremlin
 * statements so that each request can be executed within it to preserve the transaction state from one request to
 * the next.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Session {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final Bindings bindings;
    private final Settings settings;
    private final GraphManager graphManager;
    private final String session;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long configuredSessionTimeout;
    private final long configuredPerGraphCloseTimeout;
    private final boolean globalFunctionCacheEnabled;
    private final Channel boundChannel;

    private AtomicBoolean killing = new AtomicBoolean(false);
    private AtomicReference<ScheduledFuture> kill = new AtomicReference<>();

    /**
     * Each session gets its own ScriptEngine so as to isolate its configuration and the classes loaded to it.
     * This is important as it enables user interfaces built on Gremlin Server to have isolation in what
     * libraries they use and what classes exist.
     */
    private final GremlinExecutor gremlinExecutor;

    private final ThreadFactory threadFactoryWorker = ThreadFactoryUtil.create("session-%d");

    /**
     * By binding the session to run ScriptEngine evaluations in a specific thread, each request will respect
     * the ThreadLocal nature of Graph implementations.
     */
    private final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactoryWorker);

    private final ConcurrentHashMap<String, Session> sessions;

    public Session(final String session, final Context context, final ConcurrentHashMap<String, Session> sessions) {
        logger.info("New session established for {}", session);
        this.session = session;
        this.bindings = new SimpleBindings();
        this.settings = context.getSettings();
        this.graphManager = context.getGraphManager();
        this.scheduledExecutorService = context.getScheduledExecutorService();
        this.sessions = sessions;

        final Settings.ProcessorSettings processorSettings = this.settings.optionalProcessor(SessionOpProcessor.class).
                orElse(SessionOpProcessor.DEFAULT_SETTINGS);
        this.configuredSessionTimeout = Long.parseLong(processorSettings.config.getOrDefault(
                CONFIG_SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT).toString());
        this.configuredPerGraphCloseTimeout = Long.parseLong(processorSettings.config.getOrDefault(
                CONFIG_PER_GRAPH_CLOSE_TIMEOUT, DEFAULT_PER_GRAPH_CLOSE_TIMEOUT).toString());
        this.globalFunctionCacheEnabled = Boolean.parseBoolean(
                processorSettings.config.getOrDefault(CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, true).toString());

        this.gremlinExecutor = initializeGremlinExecutor().create();

        settings.scriptEngines.keySet().forEach(this::registerMetrics);

        boundChannel = context.getChannelHandlerContext().channel();
        boundChannel.closeFuture().addListener(future -> manualKill(true));
    }

    /**
     * Determines if the supplied {@code Channel} object is the same as the one bound to the {@code Session}.
     */
    public boolean isBoundTo(final Channel channel) {
        return channel == boundChannel;
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    public Bindings getBindings() {
        return bindings;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public String getSessionId() {
        return session;
    }

    public boolean acceptingRequests() {
        return !killing.get();
    }

    public void touch() {
        // if the task of killing is cancelled successfully then reset the session monitor. otherwise this session
        // has already been killed and there's nothing left to do with this session.
        kill.updateAndGet(future -> {
            if (null == future || !future.isDone()) {
                if (future != null) future.cancel(false);
                return this.scheduledExecutorService.schedule(() -> {
                        logger.info("Session {} has been idle for more than {} milliseconds - preparing to close",
                                this.session, this.configuredSessionTimeout);
                        kill(false);
                    }, this.configuredSessionTimeout, TimeUnit.MILLISECONDS);
            }

            return future;
        });
    }

    /**
     * Stops the session with call to {@link #kill(boolean)} but also stops the session expiration call which ensures
     * that the session is only killed once. See {@link #kill(boolean)} for information on how what "forcing" the
     * session kill will mean.
     */
    public void manualKill(final boolean force) {
        // seems there is a situation where kill can get nulled. seems to only happen in travis as a result of test
        // runs and i'm guessing it has something to do with a combination of shutdown and session close though i'm
        // not sure why. perhaps this "fix" just masks up a deeper problem but as i reason on it now, it seems mostly
        // bound to shutdown situations which basically means the forced end of the session anyway, so perhaps the
        // root cause isn't something that needs immediate chasing (at least until it can be shown otherwise anyway)
        Optional.ofNullable(kill.get()).ifPresent(f -> f.cancel(true));
        kill(force);
    }

    /**
     * Kills the session and rollback any uncommitted changes on transactional graphs. When "force" closed, the
     * session won't bother to try to submit transaction close commands. It will be up to the underlying graph
     * implementation to determine how it will clean up orphaned transactions. The force will try to cancel scheduled
     * jobs and interrupt any currently running ones. Interruption is not guaranteed, but an attempt will be made.
     */
    public synchronized void kill(final boolean force) {
        killing.set(true);

        // if the session has already been removed then there's no need to do this process again.  it's possible that
        // the manuallKill and the kill future could have both called kill at roughly the same time. this prevents
        // kill() from being called more than once
        if (!sessions.containsKey(session)) return;

        if (!force) {
            // when the session is killed open transaction should be rolled back
            graphManager.getGraphNames().forEach(gName -> {
                final Graph g = graphManager.getGraph(gName);
                if (g.features().graph().supportsTransactions()) {
                    // have to execute the rollback in the executor because the transaction is associated with
                    // that thread of execution from this session
                    try {
                        executor.submit(() -> {
                            if (g.tx().isOpen()) {
                                logger.info("Rolling back open transactions on {} before killing session: {}", gName, session);
                                g.tx().rollback();
                            }
                        }).get(configuredPerGraphCloseTimeout, TimeUnit.MILLISECONDS);
                    } catch (Exception ex) {
                        logger.warn(String.format("An error occurred while attempting rollback on %s when closing session: %s", gName, session), ex);
                    }
                }
            });
        } else {
            logger.info("Skipped attempt to close open graph transactions on {} - close was forced", session);
        }

        // prevent any additional requests from processing. if the kill was not "forced" then jobs were scheduled to
        // try to rollback open transactions. those jobs either timed-out or completed successfully. either way, no
        // additional jobs will be allowed, running jobs will be cancelled (if possible) and any scheduled jobs will
        // be cancelled
        executor.shutdownNow();

        sessions.remove(session);

        // once a session is dead release the gauges in the registry for it
        MetricManager.INSTANCE.getRegistry().removeMatching((s, metric) -> s.contains(session));

        logger.info("Session {} closed", session);
    }

    private GremlinExecutor.Builder initializeGremlinExecutor() {
        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .evaluationTimeout(settings.getEvaluationTimeout())
                .afterTimeout(b -> {
                    graphManager.rollbackAll();
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .afterSuccess(b -> {
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .globalBindings(graphManager.getAsBindings())
                .executorService(executor)
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // use plugins if they are present
            if (!v.plugins.isEmpty()) {
                // make sure that server related classes are available at init. the LifeCycleHook stuff will be
                // added explicitly via configuration using GremlinServerGremlinModule in the yaml. need to override
                // scriptengine settings with SessionOpProcessor specific ones as the processing for sessions is
                // different and a global setting may not make sense for a session
                if (v.plugins.containsKey(GroovyCompilerGremlinPlugin.class.getName())) {
                    v.plugins.get(GroovyCompilerGremlinPlugin.class.getName()).put(CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, globalFunctionCacheEnabled);
                } else {
                    final Map<String,Object> pluginConf = new HashMap<>();
                    pluginConf.put(CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, globalFunctionCacheEnabled);
                    v.plugins.put(GroovyCompilerGremlinPlugin.class.getName(), pluginConf);
                }

                gremlinExecutorBuilder.addPlugins(k, v.plugins);
            }
        });

        return gremlinExecutorBuilder;
    }

    private void registerMetrics(final String engineName) {
        final GremlinScriptEngine engine = gremlinExecutor.getScriptEngineManager().getEngineByName(engineName);
        MetricManager.INSTANCE.registerGremlinScriptEngineMetrics(engine, engineName, "session", session, "class-cache");
    }
}
