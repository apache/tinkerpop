package com.tinkerpop.gremlin.server.channel;

import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.AbstractGremlinChannelInitializer;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.GremlinChannelInitializer;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpChannelInitializer extends AbstractGremlinChannelInitializer {

    @Override
    public void configure(final ChannelPipeline pipeline) {
        pipeline.addLast("http-server", new HttpServerCodec());
        pipeline.addLast(gremlinGroup, "http-gremlin-handler", new HttpGremlinEndpointHandler(serializers, gremlinExecutor));
    }

    @Override
    public void finalize(final ChannelPipeline pipeline) {
        pipeline.remove(PIPELINE_OP_SELECTOR);
        pipeline.remove(PIPELINE_RESULT_ITERATOR_HANDLER);
        pipeline.remove(PIPELINE_OP_EXECUTOR);
    }
}
