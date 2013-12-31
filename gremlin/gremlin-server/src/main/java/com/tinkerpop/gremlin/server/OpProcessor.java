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
 * Interface for providing commands that websocket requests will respond to.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface OpProcessor {
    static final Logger opProcessorLogger = LoggerFactory.getLogger(OpProcessor.class);
    static final Counter errorCounter = MetricManager.INSTANCE.getCounter(name(GremlinServer.class, "errors"));

    /**
     * The name of the processor which requests must refer to "processor" field on a request.
     */
    public String getName();

    /**
     * Given the context (which contains the RequestMessage), return back a Consumer function that will be
     * executed with the context.  A typical implementation will simply check the "op" field on the RequestMessage
     * and return the Consumer function for that particular operation.
     */
    public Consumer<Context> select(final Context ctx);

    /**
     * Writes a response message as text.
     */
    public static Consumer<Context> text(final String message) {
        return (context) -> context.getChannelHandlerContext().channel().write(
                new TextWebSocketFrame(String.format("%s>>%s", context.getRequestMessage().requestId, message)));
    }

    /**
     * Writes an error response message.  All errors should be written to the client via this method as it increments
     * the error counter metrics for Gremlin Server.
     */
    public static Consumer<Context> error(final String message) {
        opProcessorLogger.warn("Error handled with this response: {}", message);
        errorCounter.inc();
        return (context) -> context.getChannelHandlerContext().channel().write(new TextWebSocketFrame(message));
    }
}
