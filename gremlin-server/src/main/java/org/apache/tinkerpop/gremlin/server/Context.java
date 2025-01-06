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
package org.apache.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The context of Gremlin Server within which a particular request is made.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;
    private final Settings settings;
    private final GraphManager graphManager;
    private final GremlinExecutor gremlinExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long requestTimeout;
    private final String materializeProperties;
    private final Object gremlinArgument;
    private HttpGremlinEndpointHandler.RequestState requestState;
    private final AtomicBoolean startedResponse = new AtomicBoolean(false);
    private ScheduledFuture<?> timeoutExecutor = null;
    private boolean timeoutExecutorGrabbed = false;
    private final Object timeoutExecutorLock = new Object();

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final GraphManager graphManager,
                   final GremlinExecutor gremlinExecutor, final ScheduledExecutorService scheduledExecutorService) {
        this(requestMessage, ctx, settings, graphManager, gremlinExecutor, scheduledExecutorService,
                HttpGremlinEndpointHandler.RequestState.NOT_STARTED);
    }

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final GraphManager graphManager,
                   final GremlinExecutor gremlinExecutor, final ScheduledExecutorService scheduledExecutorService,
                   final HttpGremlinEndpointHandler.RequestState requestState) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;

        // order of calls matter as one depends on the next
        this.gremlinArgument = requestMessage.getGremlin();
        this.requestState = requestState;
        this.requestTimeout = determineTimeout();
        this.materializeProperties = determineMaterializeProperties();
    }

    public void setTimeoutExecutor(final ScheduledFuture<?> timeoutExecutor) {
        synchronized (timeoutExecutorLock) {
            this.timeoutExecutor = timeoutExecutor;

            // Timeout was grabbed before we got here, this means the query executed before the timeout was created.
            if (timeoutExecutorGrabbed) {
                // Cancel the timeout.
                this.timeoutExecutor.cancel(true);
            }
        }
    }

    public ScheduledFuture<?> getTimeoutExecutor() {
        synchronized (timeoutExecutorLock) {
            timeoutExecutorGrabbed = true;
            return this.timeoutExecutor;
        }
    }

    /**
     * The timeout for the request. If the request is a script it examines the script for a timeout setting using
     * {@code with()}. If that is not found then it examines the request itself to see if the timeout is provided by
     * {@link Tokens#TIMEOUT_MS}. If that is not provided then the {@link Settings#evaluationTimeout} is
     * utilized as the default.
     */
    public long getRequestTimeout() {
        return requestTimeout;
    }

    public String getMaterializeProperties() {
        return materializeProperties;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * Gets the current request to Gremlin Server.
     */
    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    /**
     * Gets the Netty context.
     */
    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }


    /**
     * Gets the current configuration of Gremlin Server.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Gets the set of {@link Graph} objects configured in Gremlin Server.
     */
    public GraphManager getGraphManager() {
        return graphManager;
    }

    /**
     * Gets the executor chosen to evaluate incoming Gremlin scripts based on the request.
     */
    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    /**
     * Gets whether the server has started processing the response for this request.
     */
    public boolean getStartedResponse() { return startedResponse.get(); }

    /**
     * Signal that the server has started processing the response.
     */
    public void setStartedResponse() { startedResponse.set(true); }

    private long determineTimeout() {
        // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
        // both configurations from being submitted at the same time
        final Long timeoutMs = requestMessage.getField(Tokens.TIMEOUT_MS);
        final long seto = (null != timeoutMs) ? timeoutMs : settings.getEvaluationTimeout();

        // override the timeout if the lifecycle has a value assigned. if the script contains with(timeout)
        // options then allow that value to override what's provided on the lifecycle
        final Optional<Long> timeoutDefinedInScript = GremlinScriptChecker.parse(gremlinArgument.toString()).getTimeout();

        return timeoutDefinedInScript.orElse(seto);
    }

    private String determineMaterializeProperties() {
        final Optional<String> mp = GremlinScriptChecker.parse(gremlinArgument.toString()).getMaterializeProperties();
        if (mp.isPresent())
            return mp.get().equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS)
                    ? Tokens.MATERIALIZE_PROPERTIES_TOKENS
                    : Tokens.MATERIALIZE_PROPERTIES_ALL;

        final String materializeProperties = requestMessage.getField(Tokens.ARGS_MATERIALIZE_PROPERTIES);
        // all options except MATERIALIZE_PROPERTIES_TOKENS treated as MATERIALIZE_PROPERTIES_ALL
        return Tokens.MATERIALIZE_PROPERTIES_TOKENS.equals(materializeProperties)
                ? Tokens.MATERIALIZE_PROPERTIES_TOKENS
                : Tokens.MATERIALIZE_PROPERTIES_ALL;
    }

    public void handleDetachment(final List<Object> aggregate) {
        if (!aggregate.isEmpty() && !this.getMaterializeProperties().equals(Tokens.MATERIALIZE_PROPERTIES_ALL)) {
            final Object firstElement = aggregate.get(0);

            if (firstElement instanceof Element) {
                for (int i = 0; i < aggregate.size(); i++)
                    aggregate.set(i, ReferenceFactory.detach(aggregate.get(i)));
            } else if (firstElement instanceof AbstractTraverser) {
                for (final Object item : aggregate)
                    ((AbstractTraverser) item).detach();
            }
        }
    }

    public HttpGremlinEndpointHandler.RequestState getRequestState() {
        return requestState;
    }

    public void setRequestState(HttpGremlinEndpointHandler.RequestState requestState) {
        this.requestState = requestState;
    }
}
