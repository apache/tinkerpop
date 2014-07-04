package com.tinkerpop.gremlin.server.channel;

import com.tinkerpop.gremlin.server.AbstractChannelizer;
import com.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerCodec;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpChannelizer extends AbstractChannelizer {

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
