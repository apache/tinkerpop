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
                op = validateEvalMessage(message).orElse(evalOp());
                break;
            case ServerTokens.OPS_IMPORT:
                op = validateImportMessage(message).orElse(importOp());
                break;
            case ServerTokens.OPS_USE:
                op = validateUseMessage(message).orElse(useOp());
                break;
            case ServerTokens.OPS_DEPENDENCIES:
                op = validateUseMessage(message).orElse(depsOp());
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

    private static Consumer<Context> importOp() {
        return (context) -> {
            final RequestMessage msg = context.getRequestMessage();
            final List<String> l = (List<String>) msg.args.get(ServerTokens.ARGS_IMPORTS);
            context.getGremlinExecutor().select(msg).addImports(new HashSet<>(l));
        };
    }

    private static Consumer<Context> depsOp() {
        return (context) -> {
            final RequestMessage msg = context.getRequestMessage();
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));
            final Map dependencies = context.getGremlinExecutor().select(msg).dependencies();
            try {
                ctx.channel().write(new TextWebSocketFrame(serializer.serialize(dependencies, context)));
            } catch (Exception ex) {
                logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                        dependencies, context.getRequestMessage(), ex);
            }
        };
    }

    private static Consumer<Context> useOp() {
        return (context) -> {
            final RequestMessage msg = context.getRequestMessage();
            final List<Map<String,String>> usings = (List<Map<String,String>>) msg.args.get(ServerTokens.ARGS_COORDINATES);
            usings.forEach(c -> {
                final String group = c.get(ServerTokens.ARGS_COORDINATES_GROUP);
                final String artifact = c.get(ServerTokens.ARGS_COORDINATES_ARTIFACT);
                final String version = c.get(ServerTokens.ARGS_COORDINATES_VERSION);
                logger.info("Loading plugin [group={},artifact={},version={}]", group, artifact, version);
                context.getGremlinExecutor().select(msg).use(group, artifact, version);
                OpProcessor.text(String.format("Plugin loaded - [group=%s,artifact=%s,version=%s]", group, artifact, version)).accept(context);
            });
        };
    }

    private static Consumer<Context> evalOp() {
        return (context) -> {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final RequestMessage msg = context.getRequestMessage();
            Object o;
            try {
                o = context.getGremlinExecutor().eval(msg, context.getGraphs());
            } catch (ScriptException se) {
                logger.warn("Error while evaluating a script on request [{}]", msg);
                logger.debug("Exception from ScriptException error.", se);
                OpProcessor.error(se.getMessage()).accept(context);
                return;
            } catch (InterruptedException ie) {
                logger.warn("Thread interrupted (perhaps script ran for too long) while processing this request [{}]", msg);
                logger.debug("Exception from InterruptedException error.", ie);
                OpProcessor.error(ie.getMessage()).accept(context);
                return;
            } catch (ExecutionException ee) {
                logger.warn("Error while retrieving response from the script evaluated on request [{}]", msg);
                logger.debug("Exception from ExecutionException error.", ee.getCause());
                Throwable inner = ee.getCause();
                if (inner instanceof ScriptException)
                    inner = inner.getCause();

                OpProcessor.error(inner.getMessage()).accept(context);
                return;
            }

            Iterator itty;
            if (o instanceof Iterable)
                itty = ((Iterable) o).iterator();
            else if (o instanceof Iterator)
                itty = (Iterator) o;
            else if (o instanceof Object[])
                itty = new ArrayIterator(o);
            else if (o instanceof Map)
                itty = ((Map) o).entrySet().iterator();
            else if (o instanceof Throwable)
                itty = new SingleIterator<Object>(((Throwable) o).getMessage());
            else
                itty = new SingleIterator<>(o);

            final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));
            itty.forEachRemaining(j -> {
                try {
                    ctx.channel().write(new TextWebSocketFrame(serializer.serialize(j, context)));
                } catch (Exception ex) {
                    logger.warn("The result [{}] in the request {} could not be serialized and returned.", j, context.getRequestMessage(), ex);
                }
            });
        };
    }
}
