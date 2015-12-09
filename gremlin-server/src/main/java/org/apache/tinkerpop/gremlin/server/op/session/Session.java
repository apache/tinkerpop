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

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.LifeCycleHook;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
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

    private AtomicReference<ScheduledFuture> kill = new AtomicReference<>();

    /**
     * Each session gets its own ScriptEngine so as to isolate its configuration and the classes loaded to it.
     * This is important as it enables user interfaces built on Gremlin Server to have isolation in what
     * libraries they use and what classes exist.
     */
    private final GremlinExecutor gremlinExecutor;

    /**
     * By binding the session to run ScriptEngine evaluations in a specific thread, each request will respect
     * the ThreadLocal nature of Graph implementations.
     */
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ConcurrentHashMap<String, Session> sessions;

    public Session(final String session, final Context context, final ConcurrentHashMap<String, Session> sessions) {
        logger.info("New session established for {}", session);
        this.session = session;
        this.bindings = new SimpleBindings();
        this.settings = context.getSettings();
        this.graphManager = context.getGraphManager();
        this.scheduledExecutorService = context.getScheduledExecutorService();
        this.sessions = sessions;

        final Settings.ProcessorSettings processorSettings = this.settings.processors.stream()
                .filter(p -> p.className.equals(SessionOpProcessor.class.getCanonicalName()))
                .findAny().orElse(SessionOpProcessor.DEFAULT_SETTINGS);
        this.configuredSessionTimeout = Long.parseLong(processorSettings.config.get(SessionOpProcessor.CONFIG_SESSION_TIMEOUT).toString());

        this.gremlinExecutor = initializeGremlinExecutor().create();
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

    public void touch() {
        // if the task of killing is cancelled successfully then reset the session monitor. otherwise this session
        // has already been killed and there's nothing left to do with this session.
        final ScheduledFuture killFuture = kill.get();
        if (null == killFuture || !killFuture.isDone()) {
            if (killFuture != null) killFuture.cancel(false);
            kill.set(this.scheduledExecutorService.schedule(() -> {
                logger.info("Session {} has been idle for more than {} milliseconds - preparing to close",
                        this.session, this.configuredSessionTimeout);
                kill();
            }, this.configuredSessionTimeout, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Kills the session and rollback any uncommitted changes on transactional graphs.
     */
    public void kill() {
        // when the session is killed open transaction should be rolled back
        graphManager.getGraphs().entrySet().forEach(kv -> {
            final Graph g = kv.getValue();
            if (g.features().graph().supportsTransactions()) {
                // have to execute the rollback in the executor because the transaction is associated with
                // that thread of execution from this session
                try {
                    executor.submit(() -> {
                        logger.info("Rolling back open transactions on {} before killing session: {}", kv.getKey(), session);
                        if (g.tx().isOpen()) g.tx().rollback();
                    }).get(30000, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    logger.warn("An error occurred while attempting rollback when closing session: " + session, ex);
                }
            }
        });
        sessions.remove(session);
        logger.info("Session {} closed", session);
    }

    private GremlinExecutor.Builder initializeGremlinExecutor() {
        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .scriptEvaluationTimeout(settings.scriptEvaluationTimeout)
                .afterTimeout(b -> {
                    graphManager.rollbackAll();
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .afterSuccess(b -> {
                    this.bindings.clear();
                    this.bindings.putAll(b);
                })
                .enabledPlugins(new HashSet<>(settings.plugins))
                .globalBindings(graphManager.getAsBindings())
                .executorService(executor)
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // make sure that server related classes are available at init - not really necessary here because
            // lifecycle hooks are not executed per session, but there should be some consistency .... i guess
            v.imports.add(LifeCycleHook.class.getCanonicalName());
            v.imports.add(LifeCycleHook.Context.class.getCanonicalName());
            gremlinExecutorBuilder.addEngineSettings(k, v.imports, v.staticImports, v.scripts, v.config);
        });

        return gremlinExecutorBuilder;
    }
}
