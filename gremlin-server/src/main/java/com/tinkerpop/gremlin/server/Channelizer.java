package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.ScheduledExecutorService;

/**
 * An interface that makes it possible to plugin different Netty pipleines to Gremlin Server, enabling the use of
 * different protocols, custom security and other such functions.  A {@code Channelizer} implementation can be
 * configured in Gremlin Server with the {@code channelizer} setting in the configuration file.
 *
 * @see com.tinkerpop.gremlin.server.AbstractChannelizer
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Channelizer extends ChannelHandler {

    /**
     * This method is called just after the {@code Channelizer} is initialized.
     */
    public void init(final Settings settings, final GremlinExecutor gremlinExecutor,
                     final EventExecutorGroup gremlinGroup,
                     final Graphs graphs, final ScheduledExecutorService scheduledExecutorService);
}
