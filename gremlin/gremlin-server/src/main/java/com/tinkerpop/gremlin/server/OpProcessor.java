package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpProcessor {
    private static Optional<OpProcessor> singleton = Optional.empty();

    private OpProcessor() { }

    public Consumer<Context> select(final RequestMessage message) {
        final Consumer<Context> op;
        switch (message.op) {
            case "eval":
                op = validateEvalMessage(message).orElse(evalOp());
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
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(message));
    }

    private static Consumer<Context> evalOp() {
        return (context) -> {
            final Object o = GremlinExecutor.instance().eval(context.getRequestMessage());
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final AtomicInteger counter = new AtomicInteger(1);
            if (o instanceof Iterator) {
                ((Iterator) o).forEachRemaining(j -> ctx.channel().write(new TextWebSocketFrame(j.toString() + " " + (counter.getAndIncrement()))));
            } else if (o instanceof Iterable) {
                ((Iterable) o).forEach(j -> ctx.channel().write(new ContinuationWebSocketFrame(false, 0, j.toString())));
            } else
                ctx.channel().write(new TextWebSocketFrame(o.toString()));
        };
    }
}
