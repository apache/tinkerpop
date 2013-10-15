package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;
    private final Settings settings;

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
    }

    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }
}
