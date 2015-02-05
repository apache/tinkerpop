package com.tinkerpop.gremlin.server.op.session;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.handler.StateKey;
import com.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Operations to be used by the {@link SessionOpProcessor}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SessionOps {
    private static final Logger logger = LoggerFactory.getLogger(SessionOps.class);

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    private static ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    static {
        MetricManager.INSTANCE.getGuage(sessions::size, name(GremlinServer.class, "sessions"));
    }

    public static void evalOp(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        final Session session = getSession(context, msg);

        // place the session on the channel context so that it can be used during serialization.  in this way
        // the serialization can occur on the same thread used to execute the gremlin within the session.  this
        // is important given the threadlocal nature of Graph implementation transactions.
        context.getChannelHandlerContext().channel().attr(StateKey.SESSION).set(session);

        AbstractEvalOpProcessor.evalOp(context, session::getGremlinExecutor, () -> {
            final Bindings bindings = session.getBindings();

            // parameter bindings override session bindings if present
            Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);

            return bindings;
        });
    }

    private static Session getSession(final Context context, final RequestMessage msg) {
        final String sessionId = (String) msg.getArgs().get(Tokens.ARGS_SESSION);

        logger.debug("In-session request {} for eval for session {} in thread {}",
                msg.getRequestId(), sessionId, Thread.currentThread().getName());

        final Session session = sessions.computeIfAbsent(sessionId, k -> new Session(k, context, sessions));
        session.touch();
        return session;
    }
}
