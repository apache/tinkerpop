package com.tinkerpop.gremlin.server.op;

import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

/**
 * An {@link Exception} thrown from an {@link com.tinkerpop.gremlin.server.OpProcessor} implementation to indicate
 * some type of failure.  This failure is then routed back to the client.  Any {@link OpProcessorException} thrown
 * from an {@link com.tinkerpop.gremlin.server.OpProcessor} will be logged with an error message returned to the
 * client.  The channel will not be closed in the vent of such an exception.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpProcessorException extends Exception {
    private final TextWebSocketFrame frame;

    public OpProcessorException(final String message, final String payload) {
        super(message);
        this.frame = new TextWebSocketFrame(payload);
    }

    public OpProcessorException(final String message, final String payload, final Throwable cause) {
        super(message, cause);
        this.frame = new TextWebSocketFrame(payload);
    }

    public TextWebSocketFrame getFrame() {
        return frame;
    }
}
