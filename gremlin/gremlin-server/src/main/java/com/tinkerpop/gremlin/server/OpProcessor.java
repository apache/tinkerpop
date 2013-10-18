package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OpProcessor.class);
    private static Optional<OpProcessor> singleton = Optional.empty();

    private OpProcessor() { }

    public Consumer<Context> select(final RequestMessage message) {
        final Consumer<Context> op;
        switch (message.op) {
            case "version":
                op = (message.optionalArgs("verbose").isPresent())
                    ? text("Gremlin " + GremlinServer.getVersion() + GremlinServer.getHeader())
                    : text(GremlinServer.getVersion());
                break;
            case "eval":
                op = validateEvalMessage(message).orElse(evalOp());
                break;
            case "invalid":
                op = error(String.format("Message could not be parsed.  Check the format of the request. [%s]", message));
                break;
            default:
                op = error(String.format("Message with op code [%s] is not recognized.", message.op));
                break;
        }

        return op;
    }

    public static OpProcessor instance() {
        if (!singleton.isPresent())
            singleton = Optional.of(new OpProcessor());
        return singleton.get();
    }

    private static Optional<Consumer<Context>> validateEvalMessage(final RequestMessage message) {
        if (!message.optionalArgs("gremlin").isPresent())
            return Optional.of(error("A message with an [eval] op code requires a [gremlin] argument."));
        else
            return Optional.empty();
    }

    private static Consumer<Context> text(final String message) {
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(String.format("%s>>%s", context.getRequestMessage().requestId, message)));
    }

    private static Consumer<Context> error(final String message) {
        logger.warn(message);
        return text(message);
    }

    private static Consumer<Context> evalOp() {
        return (context) -> {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            Object o;
            try {
                o = GremlinExecutor.instance().eval(context.getRequestMessage(), context.getGraphs());
            } catch (ScriptException se) {
                logger.warn("Error while evaluating a script on request [{}]", context.getRequestMessage());
                logger.debug("Exception from ScriptException error.", se);
                error(se.getMessage()).accept(context);
                return;
            } catch (InterruptedException ie) {
                logger.warn("Thread interrupted (perhaps script ran for too long) while processing this request [{}]", context.getRequestMessage());
                logger.debug("Exception from InterruptedException error.", ie);
                error(ie.getMessage()).accept(context);
                return;
            } catch (ExecutionException ee) {
                logger.warn("Error while retrieving response from the script evaluated on request [{}]", context.getRequestMessage());
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

            itty.forEachRemaining(j -> {
                try {
                    ctx.channel().write(new TextWebSocketFrame(ResultSerializer.TO_STRING_RESULT_SERIALIZER.serialize(j, context)));
                } catch (Exception ex) {
                    logger.warn("The result [{}] in the request {} could not be serialized and returned.", j, context.getRequestMessage(), ex);
                }
            });
        };
    }
}
