package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;

/**
 * The context of Gremlin Server within which a particular request is made.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {

    /**
     * The current request.
     */
    private final RequestMessage requestMessage;

    /**
     * The Netty context.
     */
    private final ChannelHandlerContext channelHandlerContext;

    /**
     * The current configuration of Gremlin Server.
     */
    private final Settings settings;

    /**
     * The set of {@link com.tinkerpop.blueprints.Graph} objects configured in Gremlin Server.
     */
    private final GremlinServer.Graphs graphs;

    /**
     * The executor chosen to evaluate incoming Gremlin scripts based on the request.
     */
    private final GremlinExecutor gremlinExecutor;

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final GremlinServer.Graphs graphs,
                   final GremlinExecutor gremlinExecutor) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    public Settings getSettings() {
        return settings;
    }

    public GremlinServer.Graphs getGraphs() {
        return graphs;
    }

    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }
}
