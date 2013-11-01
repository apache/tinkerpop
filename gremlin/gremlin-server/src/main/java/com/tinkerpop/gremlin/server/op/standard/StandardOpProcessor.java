package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import com.tinkerpop.gremlin.server.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Standard OpProcessor implementation that handles ScriptEngine configuration and script evaluation.
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
    public Consumer<Context> select(final RequestMessage message) {
        final Consumer<Context> op;
        switch (message.op) {
            case ServerTokens.OPS_VERSION:
                op = (message.optionalArgs(ServerTokens.ARGS_VERBOSE).isPresent()) ?
                        OpProcessor.text("Gremlin " + Tokens.VERSION + GremlinServer.getHeader()) :
                        OpProcessor.text(Tokens.VERSION);
                break;
            case ServerTokens.OPS_EVAL:
                op = validateEvalMessage(message).orElse(StandardOps::evalOp);
                break;
            case ServerTokens.OPS_IMPORT:
                op = validateImportMessage(message).orElse(StandardOps::importOp);
                break;
            case ServerTokens.OPS_USE:
                op = validateUseMessage(message).orElse(StandardOps::useOp);
                break;
            case ServerTokens.OPS_DEPENDENCIES:
                op = validateUseMessage(message).orElse(StandardOps::depsOp);
                break;
            case ServerTokens.OPS_INVALID:
                op = OpProcessor.error(String.format("Message could not be parsed.  Check the format of the request. [%s]", message));
                break;
            default:
                op = OpProcessor.error(String.format("Message with op code [%s] is not recognized.", message.op));
                break;
        }

        return op;
    }

    private static Optional<Consumer<Context>> validateEvalMessage(final RequestMessage message) {
        if (!message.optionalArgs(ServerTokens.ARGS_GREMLIN).isPresent())
            return Optional.of(OpProcessor.error(String.format("A message with an [%s] op code requires a [%s] argument.",
                    ServerTokens.OPS_EVAL, ServerTokens.ARGS_GREMLIN)));
        else
            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateImportMessage(final RequestMessage message) {
        final Optional<List> l = message.optionalArgs(ServerTokens.ARGS_IMPORTS);
        if (!l.isPresent())
            return Optional.of(OpProcessor.error(String.format("A message with an [%s] op code requires a [%s] argument.",
                    ServerTokens.OPS_IMPORT, ServerTokens.ARGS_IMPORTS)));

        if (l.orElse(new ArrayList()).size() == 0)
            return Optional.of(OpProcessor.error(String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one import string specified.",
                    ServerTokens.OPS_IMPORT, ServerTokens.ARGS_IMPORTS)));
        else
            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateUseMessage(final RequestMessage message) {
        final Optional<List> l = message.optionalArgs(ServerTokens.ARGS_COORDINATES);
        if (!l.isPresent())
            return Optional.of(OpProcessor.error(String.format("A message with an [%s] op code requires a [%s] argument.",
                    ServerTokens.OPS_USE, ServerTokens.ARGS_COORDINATES)));

        final List coordinates = l.orElse(new ArrayList());
        if (coordinates.size() == 0)
            return Optional.of(OpProcessor.error(String.format(
                    "A message with an [%s] op code requires that the [%s] argument has at least one set of valid maven coordinates specified.",
                    ServerTokens.OPS_USE, ServerTokens.ARGS_COORDINATES)));

        if (!coordinates.stream().allMatch(StandardOpProcessor::validateCoordinates))
            return Optional.of(OpProcessor.error(String.format(
                    "A message with an [%s] op code requires that all [%s] specified are valid maven coordinates with a group, artifact, and version.",
                    ServerTokens.OPS_USE, ServerTokens.ARGS_COORDINATES)));
        else
            return Optional.empty();
    }

    private static boolean validateCoordinates(final Object coordinates) {
        if (!(coordinates instanceof Map))
            return false;

        final Map m = (Map) coordinates;
        return m.containsKey(ServerTokens.ARGS_COORDINATES_GROUP)
                && m.containsKey(ServerTokens.ARGS_COORDINATES_ARTIFACT)
                && m.containsKey(ServerTokens.ARGS_COORDINATES_VERSION);
    }


}
