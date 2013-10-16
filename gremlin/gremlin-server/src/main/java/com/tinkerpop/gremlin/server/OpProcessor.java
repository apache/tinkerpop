package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(OpProcessor.class);
    private static Optional<OpProcessor> singleton = Optional.empty();

    private OpProcessor() { }

    public Consumer<Context> select(final RequestMessage message) {
        final Consumer<Context> op;
        switch (message.op) {
            case "eval":
                op = validateEvalMessage(message).orElse(evalOp());
                break;
            case "invalid":
                op  = error(String.format("Message could not be parsed.  Check the format of the request. [%s]", message));
                break;
            default:
                op  = error(String.format("Message with op code [%s] is not recognized.", message.op));
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

    private static Consumer<Context> error(final String message) {
        logger.warn(message);
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(message));
    }

    private static Consumer<Context> evalOp() {
        return (context) -> {
            final Object o = GremlinExecutor.instance().eval(context.getRequestMessage(), context.getGraphs());
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
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

            itty.forEachRemaining(j -> ctx.channel().write(new TextWebSocketFrame(ResultSerializer.TO_STRING_RESULT_SERIALIZER.serialize(j, context))));
        };
    }
}
