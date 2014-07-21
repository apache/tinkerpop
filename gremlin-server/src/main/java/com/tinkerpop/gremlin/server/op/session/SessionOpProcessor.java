package com.tinkerpop.gremlin.server.op.session;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple {@link com.tinkerpop.gremlin.server.OpProcessor} implementation that handles {@code ScriptEngine}
 * script evaluation in the context of a session.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SessionOpProcessor implements OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SessionOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "session";

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        if (logger.isDebugEnabled()) logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_EVAL:
                op = validateEvalMessage(message).orElse(SessionOps::evalOp);
                break;
            case Tokens.OPS_TRAVERSE:
                op = validateTraverseMessage(message, ctx.getGraphs()).orElse(SessionOps::traverseOp);
                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgInvalid).build());
            default:
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgDefault).build());
        }

        return op;
    }

    private static Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        if (!message.optionalArgs(Tokens.ARGS_SESSION).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_SESSION);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        return Optional.empty();
    }

    private static Optional<ThrowingConsumer<Context>> validateTraverseMessage(final RequestMessage message, final Graphs graphs) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        if (!message.optionalArgs(Tokens.ARGS_GRAPH_NAME).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GRAPH_NAME);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        final String graphName = message.getArgs().get(Tokens.ARGS_GRAPH_NAME).toString();
        if (!graphs.getGraphs().containsKey(graphName)) {
            final String msg = String.format("Requested a graph by the name of [%s] that is not configured on the server.", graphName);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        return Optional.empty();
    }
}
