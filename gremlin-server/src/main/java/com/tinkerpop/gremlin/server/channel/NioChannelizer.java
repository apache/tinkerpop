package com.tinkerpop.gremlin.server.channel;

import com.tinkerpop.gremlin.server.AbstractChannelizer;
import com.tinkerpop.gremlin.server.handler.NioGremlinBinaryRequestDecoder;
import com.tinkerpop.gremlin.server.handler.NioGremlinResponseEncoder;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioChannelizer extends AbstractChannelizer {
    private static final Logger logger = LoggerFactory.getLogger(NioChannelizer.class);

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        pipeline.addLast("response-encoder", new NioGremlinResponseEncoder());
        pipeline.addLast("request-binary-decoder", new NioGremlinBinaryRequestDecoder(serializers));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-codec", LogLevel.DEBUG));
    }
}