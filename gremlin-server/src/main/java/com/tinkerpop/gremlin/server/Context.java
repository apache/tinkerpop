package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * The context of Gremlin Server within which a particular request is made.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;
    private final Settings settings;
    private final Graphs graphs;
    private final GremlinExecutor gremlinExecutor;

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final Graphs graphs,
                   final GremlinExecutor gremlinExecutor) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }


    /**
     * Gets the current request to Gremlin Server.
     */
    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    /**
     * Gets the Netty context.
     */
    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }


    /**
     * Gets the current configuration of Gremlin Server.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Gets the set of {@link com.tinkerpop.gremlin.structure.Graph} objects configured in Gremlin Server.
     */
    public Graphs getGraphs() {
        return graphs;
    }

    /**
     * Gets the executor chosen to evaluate incoming Gremlin scripts based on the request.
     */
    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }
}
