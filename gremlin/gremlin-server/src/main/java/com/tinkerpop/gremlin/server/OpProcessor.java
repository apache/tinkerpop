package com.tinkerpop.gremlin.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface OpProcessor {
    static final Logger opProcessorLogger = LoggerFactory.getLogger(OpProcessor.class);
    static final Counter errorCounter = MetricManager.INSTANCE.getCounter(name(GremlinServer.class, "errors"));

    public String getName();
    public Consumer<Context> select(final Context ctx);

    public static Consumer<Context> text(final String message) {
        return (context) -> context.getChannelHandlerContext().channel().write(
                new TextWebSocketFrame(String.format("%s>>%s", context.getRequestMessage().requestId, message)));
    }

    public static Consumer<Context> error(final String message) {
        opProcessorLogger.warn("Error handled with this response: {}", message);
        errorCounter.inc();
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(message));
    }
}
