package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OpProcessor.class);

    private static GremlinExecutor gremlinExecutor = new GremlinExecutor();

    public Consumer<Context> select(final RequestMessage message) {
        final Consumer<Context> op;
        switch (message.op) {
            case ServerTokens.OPS_VERSION:
                op = (message.optionalArgs("verbose").isPresent()) ?
                        text("Gremlin " + Tokens.VERSION + GremlinServer.getHeader()) :
                        text(Tokens.VERSION);
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
            case ServerTokens.OPS_INVALID:
                op = error(String.format("Message could not be parsed.  Check the format of the request. [%s]", message));
                break;
            default:
                op = error(String.format("Message with op code [%s] is not recognized.", message.op));
                break;
        }

        return op;
    }

    private static Optional<Consumer<Context>> validateEvalMessage(final RequestMessage message) {
        if (!message.optionalArgs("gremlin").isPresent())
            return Optional.of(error(String.format("A message with an [%s] op code requires a [gremlin] argument.", ServerTokens.OPS_EVAL)));
        else
            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateImportMessage(final RequestMessage message) {
        final Optional<List> l = message.optionalArgs("imports");
        if (!l.isPresent())
            return Optional.of(error(String.format("A message with an [%s] op code requires a [imports] argument.", ServerTokens.OPS_IMPORT)));

        if (l.orElse(new ArrayList()).size() == 0)
            return Optional.of(error(String.format("A message with an [%s] op code requires that the [imports] argument has at least one import string specified.", ServerTokens.OPS_IMPORT)));
        else
            return Optional.empty();
    }

    private static Optional<Consumer<Context>> validateUseMessage(final RequestMessage message) {
        final Optional<List> l = message.optionalArgs("coordinates");
        if (!l.isPresent())
            return Optional.of(error(String.format("A message with an [%s] op code requires a [coordinates] argument.", ServerTokens.OPS_USE)));

        final List coordinates = l.orElse(new ArrayList());
        if (coordinates.size() == 0)
            return Optional.of(error(String.format("A message with an [%s] op code requires that the [coordinates] argument has at least one set of valid maven coordinates specified.", ServerTokens.OPS_USE)));

        if (!coordinates.stream().allMatch(OpProcessor::validateCoordinates))
            return Optional.of(error(String.format("A message with an [%s] op code requires that all [coordinates] specified are valid maven coordinates with a group, artifact, and version.", ServerTokens.OPS_USE)));
        else
            return Optional.empty();
    }

    private static boolean validateCoordinates(final Object coordinates) {
        if (!(coordinates instanceof Map))
            return false;

        final Map m = (Map) coordinates;
        return m.containsKey("group") && m.containsKey("artifact") && m.containsKey("version");
    }

    private static Consumer<Context> text(final String message) {
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(String.format("%s>>%s", context.getRequestMessage().requestId, message)));
    }

    private static Consumer<Context> error(final String message) {
        logger.warn(message);
        return text(message);
    }

    private static Consumer<Context> importOp() {
        return (context) -> {
            final RequestMessage msg = context.getRequestMessage();
            final List<String> l = (List<String>) msg.args.get("imports");
            gremlinExecutor.select(msg).addImports(new HashSet<>(l));
        };
    }

    private static Consumer<Context> useOp() {
        return (context) -> {
            final RequestMessage msg = context.getRequestMessage();
            final List<Map<String,String>> usings = (List<Map<String,String>>) msg.args.get("coordinates");
            usings.forEach(c -> gremlinExecutor.select(msg).use(c.get("group"), c.get("artifact"), c.get("version")));
        };
    }

    private static Consumer<Context> evalOp() {
        return (context) -> {
            if (!gremlinExecutor.isInitialized())
                gremlinExecutor.init(context.getSettings());

            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final RequestMessage msg = context.getRequestMessage();
            Object o;
            try {
                o = gremlinExecutor.eval(msg, context.getGraphs());
            } catch (ScriptException se) {
                logger.warn("Error while evaluating a script on request [{}]", msg);
                logger.debug("Exception from ScriptException error.", se);
                error(se.getMessage()).accept(context);
                return;
            } catch (InterruptedException ie) {
                logger.warn("Thread interrupted (perhaps script ran for too long) while processing this request [{}]", msg);
                logger.debug("Exception from InterruptedException error.", ie);
                error(ie.getMessage()).accept(context);
                return;
            } catch (ExecutionException ee) {
                logger.warn("Error while retrieving response from the script evaluated on request [{}]", msg);
                logger.debug("Exception from ExecutionException error.", ee.getCause());
                Throwable inner = ee.getCause();
                if (inner instanceof ScriptException)
                    inner = inner.getCause();

                error(inner.getMessage()).accept(context);
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

            final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs("accept").orElse("text/plain"));
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
