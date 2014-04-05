package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.message.ResultCode;
import com.tinkerpop.gremlin.server.Tokens;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.server.message.ResponseMessage;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Simple {@link OpProcessor} implementation that handles {@code ScriptEngine} configuration and script evaluation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StandardOpProcessor implements OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StandardOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "";

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        if (logger.isDebugEnabled())
            logger.debug("Selecting processor for RequestMessage {}", message);

        final MessageSerializer serializer = MessageSerializer.select(
                message.<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"),
                MessageSerializer.DEFAULT_RESULT_SERIALIZER);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_VERSION:
                // todo: rework this op for sessionless
                op = null;
                break;
            case Tokens.OPS_EVAL:
                op = validateEvalMessage(message, serializer, ctx).orElse(StandardOps::evalOp);
                break;
            case Tokens.OPS_IMPORT:
                op = validateImportMessage(message, serializer, ctx).orElse(StandardOps::importOp);
                break;
            case Tokens.OPS_RESET:
                op = StandardOps::resetOp;
                break;
            case Tokens.OPS_SHOW:
                op = validateShowMessage(message, serializer, ctx).orElse(StandardOps::showOp);
                break;
            case Tokens.OPS_USE:
                op = validateUseMessage(message, serializer, ctx).orElse(StandardOps::useOp);
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

    private static Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message, final MessageSerializer serializer, final Context ctx) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        return Optional.empty();
    }

    private static Optional<ThrowingConsumer<Context>> validateImportMessage(final RequestMessage message, final MessageSerializer serializer, final Context ctx) throws OpProcessorException {
        final Optional<List> l = message.optionalArgs(Tokens.ARGS_IMPORTS);
        if (!l.isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_IMPORT, Tokens.ARGS_IMPORTS);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        if (l.orElse(new ArrayList()).size() == 0) {
            final String msg = String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one import string specified.",
                    Tokens.OPS_IMPORT, Tokens.ARGS_IMPORTS);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        return Optional.empty();
    }

    private static Optional<ThrowingConsumer<Context>> validateShowMessage(final RequestMessage message, final MessageSerializer serializer, final Context ctx) throws OpProcessorException {
        final Optional<String> infoType = message.optionalArgs(Tokens.ARGS_INFO_TYPE);
        if (!infoType.isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_SHOW, Tokens.ARGS_INFO_TYPE);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        if (!Tokens.INFO_TYPES.contains(infoType.get())) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument with one of the following values [%s].",
                    Tokens.OPS_SHOW, Tokens.ARGS_INFO_TYPE, Tokens.INFO_TYPES);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }


        return Optional.empty();

    }

    private static Optional<ThrowingConsumer<Context>> validateUseMessage(final RequestMessage message, final MessageSerializer serializer, final Context ctx) throws OpProcessorException {
        final Optional<List> l = message.optionalArgs(Tokens.ARGS_COORDINATES);
        if (!l.isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        final List coordinates = l.orElse(new ArrayList());
        if (coordinates.size() == 0) {
            final String msg = String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one set of valid maven coordinates specified.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        if (!coordinates.stream().allMatch(StandardOpProcessor::validateCoordinates)) {
            final String msg = String.format(
                    "A message with an [%s] op code requires that all [%s] specified are valid maven coordinates with a group, artifact, and version.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES);
            throw new OpProcessorException(msg, ResponseMessage.create(message).code(ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).build());
        }

        return Optional.empty();
    }

    private static boolean validateCoordinates(final Object coordinates) {
        if (!(coordinates instanceof Map))
            return false;

        final Map m = (Map) coordinates;
        return m.containsKey(Tokens.ARGS_COORDINATES_GROUP)
                && m.containsKey(Tokens.ARGS_COORDINATES_ARTIFACT)
                && m.containsKey(Tokens.ARGS_COORDINATES_VERSION);
    }

}
