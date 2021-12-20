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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedChannelizer;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handler for websockets to be used with the {@link UnifiedChannelizer}.
 */
@ChannelHandler.Sharable
public class UnifiedHandler extends SimpleChannelInboundHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(UnifiedHandler.class);

    protected final Settings settings;
    protected final GraphManager graphManager;
    protected final GremlinExecutor gremlinExecutor;
    protected final ScheduledExecutorService scheduledExecutorService;
    protected final ExecutorService sessionExecutor;
    protected final Channelizer channelizer;

    protected final ConcurrentMap<String, Session> sessions = new ConcurrentHashMap<>();

    /**
     * This may or may not be the full set of invalid binding keys.  It is dependent on the static imports made to
     * Gremlin Server.  This should get rid of the worst offenders though and provide a good message back to the
     * calling client.
     * <p/>
     * Use of {@code toUpperCase()} on the accessor values of {@link T} solves an issue where the {@code ScriptEngine}
     * ignores private scope on {@link T} and imports static fields.
     */
    protected static final Set<String> INVALID_BINDINGS_KEYS = new HashSet<>();

    static {
        INVALID_BINDINGS_KEYS.addAll(Arrays.asList(
                T.id.name(), T.key.name(),
                T.label.name(), T.value.name(),
                T.id.getAccessor(), T.key.getAccessor(),
                T.label.getAccessor(), T.value.getAccessor(),
                T.id.getAccessor().toUpperCase(), T.key.getAccessor().toUpperCase(),
                T.label.getAccessor().toUpperCase(), T.value.getAccessor().toUpperCase()));

        for (Column enumItem : Column.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Order enumItem : Order.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Operator enumItem : Operator.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Scope enumItem : Scope.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Pop enumItem : Pop.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }
    }

    public UnifiedHandler(final Settings settings, final GraphManager graphManager,
                          final GremlinExecutor gremlinExecutor,
                          final ScheduledExecutorService scheduledExecutorService,
                          final Channelizer channelizer) {
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
        this.channelizer = channelizer;
        this.sessionExecutor = gremlinExecutor.getExecutorService();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final RequestMessage msg) throws Exception {
        try {
            try {
                validateRequest(msg, graphManager);
            } catch (SessionException we) {
                ctx.writeAndFlush(we.getResponseMessage());
                return;
            }

            // ignore the close session message from older versions of the protocol
            if (msg.getOp().equals(Tokens.OPS_CLOSE)) return;

            final Optional<String> optMultiTaskSession = msg.optionalArgs(Tokens.ARGS_SESSION);
            final String sessionId = optMultiTaskSession.orElse(msg.getRequestId().toString());

            // the SessionTask is really a Context from OpProcessor. we still need the GremlinExecutor/ScriptEngine
            // config that is all rigged up into the server nicely right now so it seemed best to just keep the general
            // Context object but extend (essentially rename) it to SessionTask so that it better fits the nomenclature
            // we have here. when we drop OpProcessor stuff and rid ourselves of GremlinExecutor then we can probably
            // pare down the constructor for SessionTask further.
            final SessionTask sessionTask = new SessionTask(msg, ctx, settings, graphManager,
                    gremlinExecutor, scheduledExecutorService);

            if (sessions.containsKey(sessionId)) {
                final Session session = sessions.get(sessionId);

                // check if the session is bound to this channel, thus one client per session
                if (!session.isBoundTo(ctx.channel())) {
                    final String sessionClosedMessage = String.format("Session %s is not bound to the connecting client", sessionId);
                    final ResponseMessage response = ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(sessionClosedMessage).create();
                    ctx.writeAndFlush(response);
                    return;
                }

                // if the session is done accepting tasks then error time
                if (session.isAcceptingTasks() && !session.submitTask(sessionTask)) {
                    final String sessionClosedMessage = String.format(
                            "Session %s is no longer accepting requests as it has been closed", sessionId);
                    final ResponseMessage response = ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(sessionClosedMessage).create();
                    ctx.writeAndFlush(response);
                }
            } else {
                // determine the type of session to start - one that processes the current request only and close OR
                // one that will process this current request and ones that may arrive in the future.
                final Session session = optMultiTaskSession.isPresent() ?
                        createMultiTaskSession(sessionTask, sessionId) :
                        createSingleTaskSession(sessionTask, sessionId);

                // queue the session to startup when a thread is ready to take it
                final Future<?> sessionFuture = sessionExecutor.submit(session);
                session.setSessionFuture(sessionFuture);
                sessions.put(sessionId, session);

                // determine the max session life. for multi that's going to be "session life" and for single that
                // will be the span of the request timeout
                final long seto = sessionTask.getRequestTimeout();
                final long sessionLife = optMultiTaskSession.isPresent() ? settings.sessionLifetimeTimeout : seto;

                // if timeout is enabled when greater than zero schedule up a timeout which is a session life timeout
                // for a multi or technically a request timeout for a single. this will be cancelled when the session
                // closes by way of other reasons (i.e. success or exception) - see AbstractSession#close()
                if (seto > 0) {
                    final ScheduledFuture<?> sessionCancelFuture =
                            scheduledExecutorService.schedule(
                                    () -> session.triggerTimeout(sessionLife, optMultiTaskSession.isPresent()),
                                    sessionLife, TimeUnit.MILLISECONDS);
                    session.setSessionCancelFuture(sessionCancelFuture);
                }
            }
        } catch (RejectedExecutionException ree) {
            logger.warn(ree.getMessage());

            // generic message seems ok here? like, you would know what you were submitting on, i.e. session or
            // sessionless, when you got this error. probably don't need gory details.
            final ResponseMessage response = ResponseMessage.build(msg).code(ResponseStatusCode.TOO_MANY_REQUESTS)
                    .statusMessage("Rate limiting").create();
            ctx.writeAndFlush(response);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    protected void validateRequest(final RequestMessage message, final GraphManager graphManager) throws SessionException {
        // close message just needs to be accounted for here as of 3.5.2. it will not contain a "gremlin" arg. unified
        // channelizer basically ignores the close message otherwise
        if (!message.getOp().equals(Tokens.OPS_CLOSE) && !message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with a [%s] op code requires a [%s] argument.", message.getOp(), Tokens.ARGS_GREMLIN);
            throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (message.optionalArgs(Tokens.ARGS_SESSION).isPresent()) {
            final Optional<Object> mtx = message.optionalArgs(Tokens.ARGS_MANAGE_TRANSACTION);
            if (mtx.isPresent() && !(mtx.get() instanceof Boolean)) {
                final String msg = String.format("%s argument must be of type boolean", Tokens.ARGS_MANAGE_TRANSACTION);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final Optional<Object> msae = message.optionalArgs(Tokens.ARGS_MAINTAIN_STATE_AFTER_EXCEPTION);
            if (msae.isPresent() && !(msae.get() instanceof Boolean)) {
                final String msg = String.format("%s argument must be of type boolean", Tokens.ARGS_MAINTAIN_STATE_AFTER_EXCEPTION);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        } else {
            if (message.optionalArgs(Tokens.ARGS_MANAGE_TRANSACTION).isPresent()) {
                final String msg = String.format("%s argument only applies to requests made for sessions", Tokens.ARGS_MANAGE_TRANSACTION);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            if (message.optionalArgs(Tokens.ARGS_MAINTAIN_STATE_AFTER_EXCEPTION).isPresent()) {
                final String msg = String.format("%s argument only applies to requests made for sessions", Tokens.ARGS_MAINTAIN_STATE_AFTER_EXCEPTION);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        }

        if (message.optionalArgs(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map bindings = (Map) message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (IteratorUtils.anyMatch(bindings.keySet().iterator(), k -> null == k || !(k instanceof String))) {
                final String msg = String.format("The [%s] message is using one or more invalid binding keys - they must be of type String and cannot be null", Tokens.OPS_EVAL);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final Set<String> badBindings = IteratorUtils.set(IteratorUtils.<String>filter(bindings.keySet().iterator(), INVALID_BINDINGS_KEYS::contains));
            if (!badBindings.isEmpty()) {
                final String msg = String.format("The [%s] message supplies one or more invalid parameters key of [%s] - these are reserved names.", Tokens.OPS_EVAL, badBindings);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            // ignore control bindings that get passed in with the "#jsr223" prefix - those aren't used in compilation
            if (IteratorUtils.count(IteratorUtils.filter(bindings.keySet().iterator(), k -> !k.toString().startsWith("#jsr223"))) > settings.maxParameters) {
                final String msg = String.format("The [%s] message contains %s bindings which is more than is allowed by the server %s configuration",
                        Tokens.OPS_EVAL, bindings.size(), settings.maxParameters);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        }

        // validations for specific op codes
        if (message.getOp().equals(Tokens.OPS_EVAL)) {
            // eval must have gremlin that is type of String
            // likely a problem with the driver and how it is sending requests
            if (!(message.optionalArgs(Tokens.ARGS_GREMLIN).get() instanceof String)) {
                final String msg = String.format("A message with [%s] op code requires a [%s] argument that is of type %s.",
                        Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN, String.class.getSimpleName());
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        } else if (message.getOp().equals(Tokens.OPS_BYTECODE)) {
            // bytecode should have gremlin that is of type Bytecode
            // likely a problem with the driver and how it is sending requests
            if (!(message.optionalArgs(Tokens.ARGS_GREMLIN).get() instanceof Bytecode)) {
                final String msg = String.format("A message with [%s] op code requires a [%s] argument that is of type %s.",
                        Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN, Bytecode.class.getSimpleName());
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            // bytecode should have an alias bound
            final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
            if (!aliases.isPresent()) {
                final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            if (aliases.get().size() != 1 || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
                final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment named '%s'.",
                        Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final String traversalSourceBindingForAlias = aliases.get().values().iterator().next();
            if (!graphManager.getTraversalSourceNames().contains(traversalSourceBindingForAlias)) {
                final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
                throw new SessionException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        }
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        // only need to handle this event if the idle monitor is on
        if (!channelizer.supportsIdleMonitor()) return;

        if (evt instanceof IdleStateEvent) {
            final IdleStateEvent e = (IdleStateEvent) evt;

            // if no requests (reader) then close, if no writes from server to client then ping. clients should
            // periodically ping the server, but coming from this direction allows the server to kill channels that
            // have dead clients on the other end
            if (e.state() == IdleState.READER_IDLE) {
                logger.info("Closing channel - client is disconnected after idle period of " + settings.idleConnectionTimeout + " " + ctx.channel().id().asShortText());
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE && settings.keepAliveInterval > 0) {
                logger.info("Checking channel - sending ping to client after idle period of " + settings.keepAliveInterval + " " + ctx.channel().id().asShortText());
                ctx.writeAndFlush(channelizer.createIdleDetectionMessage());
            }
        }
    }

    /**
     * Called when creating a single task session where the provided {@link SessionTask} will be the only one to be
     * executed and can therefore take a more efficient execution path.
     */
    protected Session createSingleTaskSession(final SessionTask sessionTask, final String sessionId) {
        return new SingleTaskSession(sessionTask, sessionId, sessions);
    }

    /**
     * Called when creating a {@link Session} that will be long-lived to extend over multiple requests and therefore
     * process the provided {@link SessionTask} as well as ones that may arrive in the future.
     */
    protected Session createMultiTaskSession(final SessionTask sessionTask, final String sessionId) {
        return new MultiTaskSession(sessionTask, sessionId, sessions);
    }

    public boolean isActiveSession(final String sessionId) {
        return sessions.containsKey(sessionId);
    }

    public int getActiveSessionCount() {
        return sessions.size();
    }
}
