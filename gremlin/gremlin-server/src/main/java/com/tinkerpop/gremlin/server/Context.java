package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
    }

    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }
}
