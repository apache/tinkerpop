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
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
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
    private final String language;
    private final String batchSize;
    private final boolean bulkResults;
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
        this.requestType = RequestType.fromGremlin(gremlin);

        // Parse the script once and share the result across all per-request option resolution below. Every
        // determine*() method applies the same precedence for its option:
        //   script-embedded with() > explicit request field > request header (bulkResults only) > server/connection default
        // The individual methods are not re-documented with this rule; they only note behavior unique to that option.
        final GremlinScriptChecker.Result scriptOptions = GremlinScriptChecker.parse(gremlin);
        this.requestTimeout = determineTimeout(scriptOptions);
        this.materializeProperties = determineMaterializeProperties(scriptOptions);
        this.language = determineLanguage(scriptOptions);
        this.batchSize = determineBatchSize(scriptOptions);
        this.bulkResults = determineBulkResults(scriptOptions);
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
     * The timeout in milliseconds for the request. Resolved in the constructor (see the precedence note there).
     */
    public long getRequestTimeout() {
        return requestTimeout;
    }

    public String getMaterializeProperties() {
        return materializeProperties;
    }

    /**
     * The language resolved for the request (see the precedence note in the constructor).
     */
    public String getLanguage() {
        return language;
    }

    /**
     * The batch size resolved for the request, parsed to a positive {@code int}. Parsing happens here (rather than in
     * the {@link Context} constructor) so that an invalid value - non-numeric, non-positive, or greater than
     * {@link Integer#MAX_VALUE} - is surfaced as a bad request via {@link ProcessingException} rather than an uncaught
     * error. Callers must be on a path that turns a {@link ProcessingException} into a response (i.e. before the
     * {@code 200 OK} is committed).
     */
    public int getBatchSize() throws ProcessingException {
        final int parsed;
        try {
            parsed = Integer.parseInt(batchSize);
        } catch (NumberFormatException nfe) {
            throw new ProcessingException(GremlinError.batchSize(batchSize));
        }
        if (parsed <= 0) {
            throw new ProcessingException(GremlinError.batchSize(batchSize));
        }
        return parsed;
    }

    /**
     * Whether results should be bulked for the request (see the precedence note in the constructor).
     */
    public boolean getBulkResults() {
        return bulkResults;
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

    private long determineTimeout(final GremlinScriptChecker.Result scriptOptions) {
        final Long timeoutMillis = requestMessage.getField(Tokens.TIMEOUT_MILLIS);
        final long seto = (null != timeoutMillis) ? timeoutMillis : settings.getTimeoutMillis();
        return scriptOptions.getTimeout().orElse(seto);
    }

    private String determineMaterializeProperties(final GremlinScriptChecker.Result scriptOptions) {
        final Optional<String> mp = scriptOptions.getMaterializeProperties();
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

    private String determineLanguage(final GremlinScriptChecker.Result scriptOptions) {
        final Optional<String> lang = scriptOptions.getLanguage();
        if (lang.isPresent()) return lang.get();

        final String language = requestMessage.getField(Tokens.ARGS_LANGUAGE);
        return (null != language) ? language : "gremlin-lang";
    }

    private String determineBatchSize(final GremlinScriptChecker.Result scriptOptions) {
        // the value is kept as a raw string here and only parsed/validated in getBatchSize(), so an invalid value does
        // not throw while constructing Context (which runs outside the request's error-handling path).
        final Optional<String> bs = scriptOptions.getBatchSize();
        if (bs.isPresent()) return bs.get();

        final Integer batchSize = requestMessage.getField(Tokens.ARGS_BATCH_SIZE);
        return String.valueOf((null != batchSize) ? batchSize : settings.resultIterationBatchSize);
    }

    private boolean determineBulkResults(final GremlinScriptChecker.Result scriptOptions) {
        final Optional<Boolean> br = scriptOptions.getBulkResults();
        if (br.isPresent()) return br.get();

        final Object bulkResultsField = requestMessage.getField(Tokens.BULK_RESULTS);
        if (null != bulkResultsField) return Boolean.parseBoolean(bulkResultsField.toString());

        // the request header is a fallback below the request field. channel may be absent in unit tests.
        if (null != channelHandlerContext && null != channelHandlerContext.channel()) {
            final HttpHeaders headers = channelHandlerContext.channel().attr(StateKey.REQUEST_HEADERS).get();
            if (null != headers) {
                final String bulkResultsHeader = headers.get(Tokens.BULK_RESULTS);
                if (null != bulkResultsHeader) return Boolean.parseBoolean(bulkResultsHeader);
            }
        }

        return false;
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
