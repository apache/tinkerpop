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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor.CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED;

/**
 * A {@link Session} implementation that queues tasks given to it and executes them in a serial fashion within the
 * same thread which thus allows multiple tasks to be executed in the same transaction. The first {@link SessionTask}
 * to execute is supplied on the constructor and additional ones may be added as they arrive with
 * {@link #submitTask(SessionTask)} where they will be added to a queue where they will await execution in the thread
 * bound to this session.
 */
public class MultiTaskSession extends AbstractSession {
    private static final Logger logger = LoggerFactory.getLogger(MultiTaskSession.class);
    protected final BlockingQueue<SessionTask> queue;
    private final AtomicBoolean ending = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService;
    private final GremlinScriptEngineManager scriptEngineManager;
    private ScheduledFuture<?> requestCancelFuture;
    private Bindings bindings;

    /**
     * Creates a new {@code MultiTaskSession} object providing the initial starting {@link SessionTask} that gets
     * executed by the session when it starts.
     *
     * @param initialSessionTask The tasks that starts the session.
     * @param sessionId The id of the session
     * @param sessions The session id to {@link Session} instances mapping
     */
    public MultiTaskSession(final SessionTask initialSessionTask, final String sessionId,
                     final ConcurrentMap<String, Session> sessions) {
        super(initialSessionTask, sessionId, false, sessions);

        queue = new LinkedBlockingQueue<>(initialSessionTask.getSettings().maxSessionTaskQueueSize);

        // using a global function cache is cheaper than creating a new on per session especially if you have to
        // create a lot of sessions. it will generate a ton of throw-away objects. mostly keeping the option open
        // to not use it to preserve the ability to use the old functionality if wanted or if there is some specific
        // use case with sessions that needs it. if we wanted this could eventually become a per-request option
        // so that the client could control it as necessary and get scriptengine isolation if they need it.
        if (initialSessionTask.getSettings().useCommonEngineForSessions)
            scriptEngineManager = initialSessionTask.getGremlinExecutor().getScriptEngineManager();
        else
            scriptEngineManager = initializeGremlinExecutor(initialSessionTask).getScriptEngineManager();

        scheduledExecutorService = initialSessionTask.getScheduledExecutorService();

        // would be odd if this tossed an exception because on construction the queue should be empty,
        // but let's handle it in the same fashion as if it had been rejected by the queue itself but put up
        // an additional log because something would be rather off here if we hit this condition
        if (!submitTask(initialSessionTask)) {
            logger.error("Task {} rejected on creation of the {} for session {}",
                    initialSessionTask.getRequestMessage().getRequestId(), this.getClass().getSimpleName(),
                    getSessionId());
            final String msg = String.format("Task %s rejected from session %s",
                    initialSessionTask.getRequestMessage().getRequestId(), getSessionId());
            throw new RejectedExecutionException(msg);
        }
    }

    /**
     * Gets the script engine specific to this session which is dependent on the
     * {@link Settings#useCommonEngineForSessions} configuration.
     */
    @Override
    public GremlinScriptEngine getScriptEngine(final SessionTask sessionTask, final String language) {
        return scriptEngineManager.getEngineByName(language);
    }

    @Override
    public boolean isAcceptingTasks() {
        return !ending.get();
    }

    @Override
    public boolean submitTask(final SessionTask sessionTask) throws RejectedExecutionException {
        try {
            return isAcceptingTasks() && queue.add(sessionTask);
        } catch (IllegalStateException ise) {
            final String msg = String.format("Task %s rejected from session %s",
                    sessionTask.getRequestMessage().getRequestId(), getSessionId());
            throw new RejectedExecutionException(msg);
        }
    }

    @Override
    public void run() {
        // allow the Session to know about the thread that is running it which will be a thread from the gremlinPool
        // the thread really only has relevance once the session has started. this thread is then held below in the
        // while() loop awaiting items to arrive on the request queue. it will hold until the session is properly
        // closed by the client or through interruption/error
        this.sessionThread = Thread.currentThread();

        // there must be one item in the queue at least since addTask() gets called before the worker
        // is ever started
        SessionTask sessionTask = queue.poll();
        if (null == sessionTask)
            throw new IllegalStateException(String.format("Worker has no initial context for session: %s", getSessionId()));

        try {
            startTransaction(sessionTask);
            try {
                while (true) {
                    // schedule timeout for the current request from the queue
                    final long seto = sessionTask.getRequestTimeout();
                    requestCancelFuture = scheduledExecutorService.schedule(
                            () -> this.triggerTimeout(seto, false),
                            seto, TimeUnit.MILLISECONDS);

                    // only stop processing stuff in the queue if this session isn't configured to hold state between
                    // exceptions (i.e. the old OpProcessor way) or if this session is closing down by certain death
                    // (i.e. channel close or lifetime session timeout)
                    try {
                        process(sessionTask);
                    } catch (SessionException ex) {
                        if (!maintainStateAfterException || closeReason.get() == CloseReason.CHANNEL_CLOSED ||
                            closeReason.get() == CloseReason.SESSION_TIMEOUT) {
                            throw ex;
                        }

                        // reset the close reason as we are maintaining state
                        closeReason.set(null);

                        logger.warn(ex.getMessage(), ex);
                        sessionTask.writeAndFlush(ex.getResponseMessage());
                    }

                    // work is done within the timeout period so cancel it
                    cancelRequestTimeout();

                    sessionTask = queue.take();
                }
            } catch (Exception ex) {
                stopAcceptingRequests();

                // the current context gets its exception handled...
                handleException(sessionTask, ex);
            }
        } catch (SessionException rexex) {
            // if the close reason isn't already set then things stopped during gremlin execution somewhere and not
            // more external issues like channel close or timeouts.
            closeReason.compareAndSet(null, CloseReason.PROCESSING_EXCEPTION);

            // remaining work items in the queue are ignored since this worker is closing. must send
            // back some sort of response to satisfy the client. writeAndFlush code is different than
            // the ResponseMessage as we don't want the message to be "final" for the Context. that
            // status must be reserved for the message that caused the error
            for (SessionTask st : queue) {
                st.write(ResponseStatusCode.PARTIAL_CONTENT, ResponseMessage.build(st.getRequestMessage())
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusMessage(String.format(
                                "An earlier request [%s] failed prior to this one having a chance to execute",
                                sessionTask.getRequestMessage().getRequestId())).create());
            }

            // exception should trigger a rollback in the session. a more focused rollback may have occurred
            // during process() and the related result iteration IF transaction management was enabled on
            // the request
            closeTransactionSafely(Transaction.Status.ROLLBACK);

            // the current context could already be completed with SUCCESS and we're just waiting for another
            // one to show up while a timeout occurs or the channel closes. in these cases, this would be a valid
            // close in all likelihood so there's no reason to log or alert the client as the client already has
            // the best answer
            if (!sessionTask.isFinalResponseWritten()) {
                logger.warn(rexex.getMessage(), rexex);
                sessionTask.write(rexex.getResponseMessage());
            }
        } finally {
            // if this is a normal end to the session or if there is some general processing exception which tanks
            // the entire session or if the session life timeout is exceeded then the  session needs to be removed
            // and everything cleaned up
            if (closeReason.compareAndSet(null, CloseReason.EXIT_PROCESSING) ||
                    closeReason.get() == CloseReason.PROCESSING_EXCEPTION ||
                    closeReason.get() == CloseReason.SESSION_TIMEOUT) {
                close();

                // the session is now in a state where it is no longer in the set of current sessions so flush
                // remaining messages to the transport, if any. the remaining message should be failures from
                // the SessionException in the catch. this prevents an unlikely case where a fast client can
                // get ahead of the server and start to send messages to a technically errored out session. that
                // in itself is not necessarily bad but it makes tests fail sometimes because the tests are
                // designed to assert in the same fashion for OpProcessor and UnifiedChannelizer infrastructure
                // and they are slightly at odds with each other in certain situations.
                sessionTask.flush();
            }
        }
    }

    /**
     * This method stops incoming requests from being added to the session queue. It then cancels the session lifetime
     * timeout and then calls {@link super#close()} which will then interrupt this {@link #run()} which will clear out
     * remaining requests in the queue. This latter part of the close, where the interruption is concerned, is an
     * asynchronous operation and will not block as it finishes its processing in the "gremlinPool" thread that was
     * originally handling the work.
     */
    @Override
    public void close() {
        stopAcceptingRequests();
        cancelRequestTimeout();
        super.close();
        logger.debug("Session {} closed", getSessionId());
    }

    private void cancelRequestTimeout() {
        if (requestCancelFuture != null && !requestCancelFuture.isDone())
            requestCancelFuture.cancel(true);
        else
            logger.debug("Could not cancel request timeout for {} - {}", getSessionId(), requestCancelFuture);
    }

    private void stopAcceptingRequests() {
        if (ending.compareAndSet(false, true))
            cancel(true);
    }

    @Override
    protected Bindings getWorkerBindings() throws SessionException {
        if (null == bindings)
            bindings = super.getWorkerBindings();
        return this.bindings;
    }

    protected GremlinExecutor initializeGremlinExecutor(final SessionTask sessionTask) {
        final Settings settings = sessionTask.getSettings();
        final ExecutorService executor = sessionTask.getGremlinExecutor().getExecutorService();
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

    @Override
    public String toString() {
        return String.format("%s - session: %s", MultiTaskSession.class.getSimpleName(), getSessionId());
    }
}
