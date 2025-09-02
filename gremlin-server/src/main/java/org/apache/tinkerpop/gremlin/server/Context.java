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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptChecker;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.WsUserAgentHandler;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
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
    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;
    private final Settings settings;
    private final GraphManager graphManager;
    private final GremlinExecutor gremlinExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean finalResponseWritten = new AtomicBoolean();
    private final long requestTimeout;
    private final String materializeProperties;
    private final RequestContentType requestContentType;
    private final Object gremlinArgument;
    private final AtomicBoolean startedResponse = new AtomicBoolean(false);
    private ScheduledFuture<?> timeoutExecutor = null;
    private boolean timeoutExecutorGrabbed = false;
    private final Object timeoutExecutorLock = new Object();

    /**
     * The type of the request as determined by the contents of {@link Tokens#ARGS_GREMLIN}.
     */
    public enum RequestContentType {
        /**
         * Contents is of type {@link Bytecode}.
         */
        BYTECODE,

        /**
         * Contents is of type {@code String}.
         */
        SCRIPT,

        /**
         * Contents are not of a type that is expected.
         */
        UNKNOWN
    }

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
        this.gremlinArgument = requestMessage.getArgs().get(Tokens.ARGS_GREMLIN);
        this.requestContentType = determineRequestContents();
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
     * {@link Tokens#ARGS_EVAL_TIMEOUT}. If that is not provided then the {@link Settings#evaluationTimeout} is
     * utilized as the default.
     */
    public long getRequestTimeout() {
        return requestTimeout;
    }

    public String getMaterializeProperties() {
        return materializeProperties;
    }

    public boolean isFinalResponseWritten() {
        return this.finalResponseWritten.get();
    }

    public RequestContentType getRequestContentType() {
        return requestContentType;
    }

    public Object getGremlinArgument() {
        return gremlinArgument;
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
     * Returns the user agent (if any) which was sent from the client during the web socket handshake.
     * Returns empty string if no user agent exists
     */
    public String getUserAgent() {
        return getChannelHandlerContext().channel().hasAttr(WsUserAgentHandler.USER_AGENT_ATTR_KEY) ?
                getChannelHandlerContext().channel().attr(WsUserAgentHandler.USER_AGENT_ATTR_KEY).get() : "";
    }

    /**
     * Gets whether the server has started processing the response for this request.
     */
    public boolean getStartedResponse() { return startedResponse.get(); }

    /**
     * Signal that the server has started processing the response.
     */
    public void setStartedResponse() { startedResponse.set(true); }

    /**
     * Writes a default timeout error response message to the underlying channel.
     */
    public void sendTimeoutResponse() {
        sendTimeoutResponse(String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout", requestMessage));
    }

    /**
     * Writes a specific timeout error response message to the underlying channel.
     */
    public void sendTimeoutResponse(final String message) {
        logger.warn(message);
        writeAndFlush(ResponseMessage.build(requestMessage)
                .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                .statusMessage(message)
                .statusAttributeException(new InterruptedException()).create());
    }

    /**
     * Writes a response message to the underlying channel while ensuring that at most one
     * {@link ResponseStatusCode#isFinalResponse() final} response is written.
     * <p>Note: this method should be used instead of writing to the channel directly when multiple threads
     * are expected to produce response messages concurrently.</p>
     * <p>Attempts to write more than one final response message will be ignored.</p>
     * @see #writeAndFlush(ResponseStatusCode, Object)
     */
    public void writeAndFlush(final ResponseMessage message) {
        writeAndFlush(message.getStatus().getCode(), message);
    }

    /**
     * Writes a response message to the underlying channel while ensuring that at most one
     * {@link ResponseStatusCode#isFinalResponse() final} response is written.
     * <p>The caller must make sure that the provided response status code matches the content of the message.</p>
     * <p>Note: this method should be used instead of writing to the channel directly when multiple threads
     * are expected to produce response messages concurrently.</p>
     * <p>Attempts to write more than one final response message will be ignored.</p>
     * @see #writeAndFlush(ResponseMessage)
     */
    public void writeAndFlush(final ResponseStatusCode code, final Object responseMessage) {
        writeAndMaybeFlush(code, responseMessage, true);
    }

    public void write(final ResponseMessage message) {
        this.write(message.getStatus().getCode(), message);
    }

    public void write(final ResponseStatusCode code, final Object responseMessage) {
        writeAndMaybeFlush(code, responseMessage, false);
    }

    /**
     * Flushes messages to the underlying transport.
     */
    public void flush() {
        this.getChannelHandlerContext().flush();
    }

    private void writeAndMaybeFlush(final ResponseStatusCode code, final Object responseMessage, final boolean flush) {
        final boolean messageIsFinal = code.isFinalResponse();
        if (finalResponseWritten.compareAndSet(false, messageIsFinal)) {
            this.getChannelHandlerContext().write(responseMessage);
            if (flush) this.getChannelHandlerContext().flush();
        } else {
            if (responseMessage instanceof Frame) {
                ((Frame) responseMessage).tryRelease();
            }

            final String logMessage = String.format("Another final response message was already written for request %s, ignoring response code: %s",
                    this.getRequestMessage().getRequestId(), code);
            logger.warn(logMessage);
        }
    }

    private RequestContentType determineRequestContents() {
        if (gremlinArgument instanceof Bytecode)
            return RequestContentType.BYTECODE;
        else if (gremlinArgument instanceof String)
            return RequestContentType.SCRIPT;
        else
            return RequestContentType.UNKNOWN;
    }

    private long determineTimeout() {
        // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
        // both configurations from being submitted at the same time
        final Map<String, Object> args = requestMessage.getArgs();
        final long seto = args.containsKey(Tokens.ARGS_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_EVAL_TIMEOUT)).longValue() : settings.getEvaluationTimeout();

        // override the timeout if the lifecycle has a value assigned. if the script contains with(timeout)
        // options then allow that value to override what's provided on the lifecycle
        final Optional<Long> timeoutDefinedInScript = requestContentType == RequestContentType.SCRIPT ?
                GremlinScriptChecker.parse(gremlinArgument.toString()).getTimeout() : Optional.empty();

        return timeoutDefinedInScript.orElse(seto);
    }

    private String determineMaterializeProperties() {
        // with() in Script request has the highest priority
        if (requestContentType == RequestContentType.SCRIPT) {
            final Optional<String> mp = GremlinScriptChecker.parse(gremlinArgument.toString()).getMaterializeProperties();
            if (mp.isPresent())
                return mp.get().equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS)
                        ? Tokens.MATERIALIZE_PROPERTIES_TOKENS
                        : Tokens.MATERIALIZE_PROPERTIES_ALL;
        }

        final Map<String, Object> args = requestMessage.getArgs();
        // all options except MATERIALIZE_PROPERTIES_TOKENS treated as MATERIALIZE_PROPERTIES_ALL
        return args.containsKey(Tokens.ARGS_MATERIALIZE_PROPERTIES)
                && args.get(Tokens.ARGS_MATERIALIZE_PROPERTIES).equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS)
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
}
