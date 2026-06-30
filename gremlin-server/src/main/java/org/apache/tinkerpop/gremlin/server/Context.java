/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

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
    private final RequestType requestType;
    private ScheduledFuture<?> timeoutExecutor = null;
    private boolean timeoutExecutorGrabbed = false;
    private final Object timeoutExecutorLock = new Object();
    private String transactionId; // initially null for non-transactional requests and begin() calls; set after transaction creation.
    private Map<String, Object> parameters = new HashMap<>(); // only available after string parameters are parsed by grammar.
    // Set by the transaction's lifetime cap (on the scheduler thread) before it interrupts this request's operation, and
    // read as the interrupt unwinds the operation (on the transaction worker thread) to report an accurate
    // transaction-timeout error rather than a generic evaluation timeout. volatile for cross-thread visibility.
    private volatile boolean closedByLifetimeCap = false;

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final GraphManager graphManager,
                   final GremlinExecutor gremlinExecutor, final ScheduledExecutorService scheduledExecutorService) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;

        // order of calls matter as one depends on the next
        final String gremlin = requestMessage.getGremlin();
        this.gremlinArgument = gremlin;
        this.requestType = RequestType.fromGremlin(gremlin);
        this.requestTimeout = determineTimeout();
        this.materializeProperties = determineMaterializeProperties();
        this.transactionId = requestMessage.getField(Tokens.ARGS_TRANSACTION_ID);
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

    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Returns {@code true} if this request is a transaction begin control command.
     */
    public boolean isTransactionBegin() {
        return requestType == RequestType.BEGIN_TX;
    }

    /**
     * Returns {@code true} if this request is a transaction commit control command.
     */
    public boolean isTransactionCommit() {
        return requestType == RequestType.COMMIT_TX;
    }

    /**
     * Returns {@code true} if this request is a transaction rollback control command.
     */
    public boolean isTransactionRollback() {
        return requestType == RequestType.ROLLBACK_TX;
    }

    public void setTransactionId(final String transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * Marks this request's operation as having been interrupted because its transaction hit its absolute lifetime cap.
     * Set by the transaction's lifetime cap before it interrupts the operation.
     */
    public void setClosedByLifetimeCap(final boolean closedByLifetimeCap) {
        this.closedByLifetimeCap = closedByLifetimeCap;
    }

    /**
     * Returns {@code true} if this request's operation was interrupted by its transaction's absolute lifetime cap, in
     * which case the resulting interrupt should be reported as a transaction timeout rather than an evaluation timeout.
     */
    public boolean isClosedByLifetimeCap() {
        return closedByLifetimeCap;
    }

    public Map<String, Object> getParameters() {
        return this.parameters;
    }

    public void setParameters(final Map<String, Object> parameters) {
        this.parameters = parameters;
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
            for (int i = 0; i < aggregate.size(); i++) {
                final Object o = aggregate.get(i);
                if (o instanceof AbstractTraverser) {
                    ((AbstractTraverser) o).detach();
                } else {
                    aggregate.set(i, ReferenceFactory.detach(o));
                }
            }
        }
    }

    /**
     * Classifies an HTTP request by the kind of work it performs. Transaction control requests reuse the canonical
     * Gremlin idioms ({@code g.tx().begin()} etc.) as protocol signals rather than evaluating them, because the
     * ThreadLocal-bound nature of graph transactions forces the server to route the underlying graph operation onto
     * a dedicated per-transaction thread instead of the shared eval path. The match is therefore against fixed,
     * case-sensitive tokens — the same spelling the script engine would accept — not a grammar parse.
     */
    private enum RequestType {
        BEGIN_TX,
        COMMIT_TX,
        ROLLBACK_TX,
        EVAL;

        private static final String BEGIN = "g.tx().begin()";
        private static final String COMMIT = "g.tx().commit()";
        private static final String ROLLBACK = "g.tx().rollback()";

        /**
         * Classifies a gremlin string into a {@link RequestType}. Leading/trailing whitespace is tolerated, but the
         * token match is case-sensitive so that the routing decision cannot disagree with how the script engine would
         * parse the same string.
         */
        private static RequestType fromGremlin(final String gremlin) {
            if (gremlin == null) return EVAL;
            final String trimmed = gremlin.trim();
            if (trimmed.equals(BEGIN)) return BEGIN_TX;
            if (trimmed.equals(COMMIT)) return COMMIT_TX;
            if (trimmed.equals(ROLLBACK)) return ROLLBACK_TX;
            return EVAL;
        }
    }
}
