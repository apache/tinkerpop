package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.RequestMessage;
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.ResultSerializer;
import com.tinkerpop.gremlin.server.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Standard {@link OpProcessor} implementation that handles {@code ScriptEngine} configuration and script evaluation.
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
    public Consumer<Context> select(final Context ctx) {
        final RequestMessage message = ctx.getRequestMessage();
        if (logger.isDebugEnabled())
            logger.debug("Selecting processor for RequestMessage {}", message);

        final ResultSerializer serializer = ResultSerializer.select(message.<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"));

        final Consumer<Context> op;
        switch (message.op) {
            case Tokens.OPS_VERSION:
                op = (message.optionalArgs(Tokens.ARGS_VERBOSE).isPresent()) ?
                        OpProcessor.text("Gremlin " + com.tinkerpop.gremlin.Tokens.VERSION + GremlinServer.getHeader()) :
                        OpProcessor.text(com.tinkerpop.gremlin.Tokens.VERSION);
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
                op = OpProcessor.error(serializer.serialize(String.format("Message could not be parsed.  Check the format of the request. [%s]", message), ResultCode.REQUEST_ERROR_MALFORMED_REQUEST, ctx));
                break;
            default:
                op = OpProcessor.error(serializer.serialize(String.format("Message with op code [%s] is not recognized.", message.op), ResultCode.REQUEST_ERROR_MALFORMED_REQUEST, ctx));
                break;
        }

        return op;
    }

    private static Optional<Consumer<Context>> validateEvalMessage(final RequestMessage message, final ResultSerializer serializer, final Context ctx) {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent())
            return Optional.of(OpProcessor.error(serializer.serialize(String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateImportMessage(final RequestMessage message, final ResultSerializer serializer, final Context ctx) {
        final Optional<List> l = message.optionalArgs(Tokens.ARGS_IMPORTS);
        if (!l.isPresent())
            return Optional.of(OpProcessor.error(serializer.serialize(String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_IMPORT, Tokens.ARGS_IMPORTS), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

        if (l.orElse(new ArrayList()).size() == 0)
            return Optional.of(OpProcessor.error(serializer.serialize(String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one import string specified.",
                    Tokens.OPS_IMPORT, Tokens.ARGS_IMPORTS), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateShowMessage(final RequestMessage message, final ResultSerializer serializer, final Context ctx) {
        final Optional<String> infoType = message.optionalArgs(Tokens.ARGS_INFO_TYPE);
        if (!infoType.isPresent())
            return Optional.of(OpProcessor.error(serializer.serialize(String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_SHOW, Tokens.ARGS_INFO_TYPE), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

        if (!Tokens.INFO_TYPES.contains(infoType.get()))
            return Optional.of(OpProcessor.error(serializer.serialize(String.format("A message with an [%s] op code requires a [%s] argument with one of the following values [%s].",
                    Tokens.OPS_SHOW, Tokens.ARGS_INFO_TYPE, Tokens.INFO_TYPES), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

        if (infoType.get().equals(Tokens.ARGS_INFO_TYPE_VARIABLES) && !message.optionalSessionId().isPresent())
            return Optional.of(OpProcessor.error(serializer.serialize(String.format("A message with an [%s] op code and [%s] value of [%s] is can only be used with in-session requests as bindings are not 'kept' on the server in non-session requests.",
                    Tokens.OPS_SHOW, Tokens.ARGS_INFO_TYPE, Tokens.ARGS_INFO_TYPE_VARIABLES), ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, ctx)));

        return Optional.empty();

    }

    private static Optional<Consumer<Context>> validateUseMessage(final RequestMessage message, final ResultSerializer serializer, final Context ctx) {
        final Optional<List> l = message.optionalArgs(Tokens.ARGS_COORDINATES);
        if (!l.isPresent())
            return Optional.of(OpProcessor.error(String.format("A message with an [%s] op code requires a [%s] argument.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES)));

        final List coordinates = l.orElse(new ArrayList());
        if (coordinates.size() == 0)
            return Optional.of(OpProcessor.error(String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one set of valid maven coordinates specified.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES)));

        if (!coordinates.stream().allMatch(StandardOpProcessor::validateCoordinates))
            return Optional.of(OpProcessor.error(String.format(
                    "A message with an [%s] op code requires that all [%s] specified are valid maven coordinates with a group, artifact, and version.",
                    Tokens.OPS_USE, Tokens.ARGS_COORDINATES)));

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
