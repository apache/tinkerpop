package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Channelizer extends ChannelHandler {
    public void init(final Settings settings, final GremlinExecutor gremlinExecutor,
                     final EventExecutorGroup gremlinGroup,
                     final Graphs graphs);
}
