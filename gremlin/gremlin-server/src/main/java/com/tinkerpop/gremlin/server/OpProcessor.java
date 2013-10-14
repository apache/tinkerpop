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

    public Optional<Consumer<Context>> select(final RequestMessage message) {
        final Optional<Consumer<Context>> op;
        switch (message.op) {
            case "eval":
                op = Optional.of(evalOp());
                break;
            default:
                op  = Optional.empty();
                break;
        }

        return op;
    }

    public static OpProcessor instance() {
        if (!singleton.isPresent())
            singleton = Optional.of(new OpProcessor());
        return singleton.get();
    }

    private Consumer<Context> evalOp() {
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
