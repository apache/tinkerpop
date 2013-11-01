package com.tinkerpop.gremlin.server;

import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface OpProcessor {
    static final Logger opProcessorLogger = LoggerFactory.getLogger(OpProcessor.class);

    public String getName();
    public Consumer<Context> select(final RequestMessage message);

    public static Consumer<Context> text(final String message) {
        return (context) -> context.getChannelHandlerContext().channel().write(
                new TextWebSocketFrame(String.format("%s>>%s", context.getRequestMessage().requestId, message)));
    }

    public static Consumer<Context> error(final String message) {
        opProcessorLogger.warn(message);
        return text(message);
    }
}
