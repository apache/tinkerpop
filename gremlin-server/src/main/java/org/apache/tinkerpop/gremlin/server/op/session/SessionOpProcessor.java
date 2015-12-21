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

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple {@link org.apache.tinkerpop.gremlin.server.OpProcessor} implementation that handles
 * {@code ScriptEngine} script evaluation in the context of a session. Note that this processor will
 * also take a "close" op to kill the session and rollback any incomplete transactions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SessionOpProcessor extends AbstractEvalOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SessionOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "session";

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    protected static ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    static {
        MetricManager.INSTANCE.getGuage(sessions::size, name(GremlinServer.class, "sessions"));
    }

    /**
     * Configuration setting for how long a session will be available before it timesout.
     */
    public static final String CONFIG_SESSION_TIMEOUT = "sessionTimeout";

    /**
     * Default timeout for a session is eight hours.
     */
    public static final long DEFAULT_SESSION_TIMEOUT = 28800000l;

    static final Settings.ProcessorSettings DEFAULT_SETTINGS = new Settings.ProcessorSettings();

    static {
        DEFAULT_SETTINGS.className = SessionOpProcessor.class.getCanonicalName();
        DEFAULT_SETTINGS.config = new HashMap<String, Object>() {{
            put(CONFIG_SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT);
        }};
    }

    public SessionOpProcessor() {
        super(false);
    }

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    /**
     * Session based requests accept a "close" operator in addition to "eval".  A close will trigger the session to be
     * killed and any uncommitted transaction to be rolled-back.
     */
    @Override
    public Optional<ThrowingConsumer<Context>> selectOther(final RequestMessage requestMessage)  throws OpProcessorException {
        if (requestMessage.getOp().equals(Tokens.OPS_CLOSE)) {
            // this must be an in-session request
            if (!requestMessage.optionalArgs(Tokens.ARGS_SESSION).isPresent()) {
                final String msg = String.format("A message with an [%s] op code requires a [%s] argument", Tokens.OPS_CLOSE, Tokens.ARGS_SESSION);
                throw new OpProcessorException(msg, ResponseMessage.build(requestMessage).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            return Optional.of(ctx -> {
                // validate the session is present and then remove it if it is.
                final Session sessionToClose = sessions.get(requestMessage.getArgs().get(Tokens.ARGS_SESSION).toString());
                if (null == sessionToClose) {
                    final String msg = String.format("There was no session named %s to close", requestMessage.getArgs().get(Tokens.ARGS_SESSION).toString());
                    throw new OpProcessorException(msg, ResponseMessage.build(requestMessage).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                sessionToClose.kill();
            });
        } else {
            return Optional.empty();
        }
    }

    @Override
    public ThrowingConsumer<Context> getEvalOp() {
        return this::evalOp;
    }

    @Override
    protected Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message) throws OpProcessorException {
        super.validateEvalMessage(message);

        if (!message.optionalArgs(Tokens.ARGS_SESSION).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument", Tokens.OPS_EVAL, Tokens.ARGS_SESSION);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return Optional.empty();
    }

    @Override
    public void close() throws Exception {
       sessions.values().forEach(Session::kill);
    }

    protected void evalOp(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        final Session session = getSession(context, msg);

        // place the session on the channel context so that it can be used during serialization.  in this way
        // the serialization can occur on the same thread used to execute the gremlin within the session.  this
        // is important given the threadlocal nature of Graph implementation transactions.
        context.getChannelHandlerContext().channel().attr(StateKey.SESSION).set(session);

        evalOpInternal(context, session::getGremlinExecutor, () -> {
            final Bindings bindings = session.getBindings();

            // parameter bindings override session bindings if present
            Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);

            return bindings;
        });
    }

    /**
     * Examines the {@link RequestMessage} and extracts the session token. The session is then either found or a new
     * one is created.
     */
    protected static Session getSession(final Context context, final RequestMessage msg) {
        final String sessionId = (String) msg.getArgs().get(Tokens.ARGS_SESSION);

        logger.debug("In-session request {} for eval for session {} in thread {}",
                msg.getRequestId(), sessionId, Thread.currentThread().getName());

        final Session session = sessions.computeIfAbsent(sessionId, k -> new Session(k, context, sessions));
        session.touch();
        return session;
    }
}
