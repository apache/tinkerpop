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
package org.apache.tinkerpop.gremlin.server.handler;

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED;

/**
 * A {@link Rexster} implementation that queues tasks given to it and executes them in a serial fashion within the
 * same thread which thus allows multiple tasks to be executed in the same transaction.
 */
public class MultiRexster extends AbstractRexster {
    private static final Logger logger = LoggerFactory.getLogger(MultiRexster.class);
    protected final BlockingQueue<Context> queue = new LinkedBlockingQueue<>();
    private ScheduledFuture<?> requestCancelFuture;
    private Bindings bindings;
    private final AtomicBoolean ending = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService;
    private final GremlinScriptEngineManager scriptEngineManager;

    MultiRexster(final Context gremlinContext, final String sessionId,
                 final ConcurrentMap<String, Rexster> sessions) {
        super(gremlinContext, sessionId, false, sessions);

        // using a global function cache is cheaper than creating a new on per session especially if you have to
        // create a lot of sessions. it will generate a ton of throw-away objects. mostly keeping the option open
        // to not use it to preserve the ability to use the old functionality if wanted or if there is some specific
        // use case with sessions that needs it. if we wanted this could eventually become a per-request option
        // so that the client could control it as necessary and get scriptengine isolation if they need it.
        if (gremlinContext.getSettings().useCommonEngineForSessions)
            scriptEngineManager = gremlinContext.getGremlinExecutor().getScriptEngineManager();
        else
            scriptEngineManager = initializeGremlinExecutor(gremlinContext).getScriptEngineManager();

        scheduledExecutorService = gremlinContext.getScheduledExecutorService();
        addTask(gremlinContext);
    }

    @Override
    public GremlinScriptEngine getScriptEngine(final Context gremlinContext, final String language) {
        return scriptEngineManager.getEngineByName(language);
    }

    @Override
    public boolean acceptingTasks() {
        return !ending.get();
    }

    @Override
    public void addTask(final Context gremlinContext) {
        if (acceptingTasks())
            queue.offer(gremlinContext);
    }

    @Override
    public void run() {
        // there must be one item in the queue at least since addTask() gets called before the worker
        // is ever started
        Context currentGremlinContext = queue.poll();
        if (null == currentGremlinContext)
            throw new IllegalStateException(String.format("Worker has no initial context for session: %s", getSessionId()));

        try {
            startTransaction(currentGremlinContext);
            try {
                while (true) {
                    // schedule timeout for the current request from the queue
                    final long seto = currentGremlinContext.getRequestTimeout();
                    requestCancelFuture = scheduledExecutorService.schedule(
                            () -> this.triggerTimeout(seto, false),
                            seto, TimeUnit.MILLISECONDS);

                    // only stop processing stuff in the queue if this Rexster isn't configured to hold state between
                    // exceptions (i.e. the old OpProcessor way) or if this Rexster is closing down by certain death
                    // (i.e. channel close or lifetime session timeout)
                    try {
                        process(currentGremlinContext);
                    } catch (RexsterException ex) {
                        if (!maintainStateAfterException || closeReason.get() == CloseReason.CHANNEL_CLOSED ||
                            closeReason.get() == CloseReason.SESSION_TIMEOUT) {
                            throw ex;
                        }

                        // reset the close reason as we are maintaining state
                        closeReason.set(null);

                        logger.warn(ex.getMessage(), ex);
                        currentGremlinContext.writeAndFlush(ex.getResponseMessage());
                    }

                    // work is done within the timeout period so cancel it
                    cancelRequestTimeout();

                    currentGremlinContext = queue.take();
                }
            } catch (Exception ex) {
                stopAcceptingRequests();

                // the current context gets its exception handled...
                handleException(currentGremlinContext, ex);
            }
        } catch (RexsterException rexex) {
            // remaining work items in the queue are ignored since this worker is closing. must send
            // back some sort of response to satisfy the client. writeAndFlush code is different than
            // the ResponseMessage as we don't want the message to be "final" for the Context. that
            // status must be reserved for the message that caused the error
            for (Context gctx : queue) {
                gctx.writeAndFlush(ResponseStatusCode.PARTIAL_CONTENT, ResponseMessage.build(gctx.getRequestMessage())
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusMessage(String.format(
                                "An earlier request [%s] failed prior to this one having a chance to execute",
                                currentGremlinContext.getRequestMessage().getRequestId())).create());
            }

            // exception should trigger a rollback in the session. a more focused rollback may have occurred
            // during process() and the related result iteration IF transaction management was enabled on
            // the request
            closeTransactionSafely(Transaction.Status.ROLLBACK);

            // the current context could already be completed with SUCCESS and we're just waiting for another
            // one to show up while a timeout occurs or the channel closes. in these cases, this would be a valid
            // close in all likelihood so there's no reason to log or alert the client as the client already has
            // the best answer
            if (!currentGremlinContext.isFinalResponseWritten()) {
                logger.warn(rexex.getMessage(), rexex);
                currentGremlinContext.writeAndFlush(rexex.getResponseMessage());
            }
        } finally {
            // if this is a normal end to the session or if the session life timeout is exceeded then the
            // session needs to be removed and everything cleaned up
            if (closeReason.compareAndSet(null, CloseReason.NORMAL) || closeReason.get() == CloseReason.SESSION_TIMEOUT) {
                close();
            }
        }
    }

    @Override
    public void close() {
        ending.set(true);
        cancelRequestTimeout();
        super.close();
        logger.info("Session {} closed", getSessionId());
    }

    private void cancelRequestTimeout() {
        if (requestCancelFuture != null && !requestCancelFuture.isDone())
            requestCancelFuture.cancel(true);
    }

    private void stopAcceptingRequests() {
        ending.set(true);
        cancel(true);
    }

    @Override
    protected Bindings getWorkerBindings() throws RexsterException {
        if (null == bindings)
            bindings = super.getWorkerBindings();
        return this.bindings;
    }

    protected GremlinExecutor initializeGremlinExecutor(final Context gremlinContext) {
        final Settings settings = gremlinContext.getSettings();
        final ExecutorService executor = gremlinContext.getGremlinExecutor().getExecutorService();
        final boolean useGlobalFunctionCache = settings.useGlobalFunctionCacheForSessions;

        // these initial settings don't matter so much as we don't really execute things through the
        // GremlinExecutor directly. Just doing all this setup to make GremlinExecutor do the work of
        // rigging up the GremlinScriptEngineManager.
        final GremlinExecutor.Builder gremlinExecutorBuilder = GremlinExecutor.build()
                .evaluationTimeout(settings.getEvaluationTimeout())
                .executorService(executor)
                .globalBindings(graphManager.getAsBindings())
                .scheduledExecutorService(scheduledExecutorService);

        settings.scriptEngines.forEach((k, v) -> {
            // use plugins if they are present
            if (!v.plugins.isEmpty()) {
                // make sure that server related classes are available at init. the LifeCycleHook stuff will be
                // added explicitly via configuration using GremlinServerGremlinModule in the yaml. need to override
                // scriptengine settings with SessionOpProcessor specific ones as the processing for sessions is
                // different and a global setting may not make sense for a session
                if (v.plugins.containsKey(GroovyCompilerGremlinPlugin.class.getName())) {
                    v.plugins.get(GroovyCompilerGremlinPlugin.class.getName()).put(CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, useGlobalFunctionCache);
                } else {
                    final Map<String,Object> pluginConf = new HashMap<>();
                    pluginConf.put(CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, useGlobalFunctionCache);
                    v.plugins.put(GroovyCompilerGremlinPlugin.class.getName(), pluginConf);
                }

                gremlinExecutorBuilder.addPlugins(k, v.plugins);
            }
        });

        return gremlinExecutorBuilder.create();
    }
}
