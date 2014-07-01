package com.tinkerpop.gremlin.server.channel;

import com.tinkerpop.gremlin.server.AbstractGremlinChannelInitializer;
import com.tinkerpop.gremlin.server.handler.GremlinBinaryRequestDecoder;
import com.tinkerpop.gremlin.server.handler.GremlinResponseEncoder;
import com.tinkerpop.gremlin.server.handler.GremlinTextRequestDecoder;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketChannelInitializer extends AbstractGremlinChannelInitializer {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketChannelInitializer.class);

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        logger.debug("HttpRequestDecoder settings - maxInitialLineLength={}, maxHeaderSize={}, maxChunkSize={}",
                settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize);
        pipeline.addLast("http-request-decoder", new HttpRequestDecoder(settings.maxInitialLineLength, settings.maxHeaderSize, settings.maxChunkSize));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-decoder-aggregator", LogLevel.DEBUG));

        logger.debug("HttpObjectAggregator settings - maxContentLength={}, maxAccumulationBufferComponents={}",
                settings.maxContentLength, settings.maxAccumulationBufferComponents);
        final HttpObjectAggregator aggregator = new HttpObjectAggregator(settings.maxContentLength);
        aggregator.setMaxCumulationBufferComponents(settings.maxAccumulationBufferComponents);
        pipeline.addLast("aggregator", aggregator);

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("http-response-encoder", new HttpResponseEncoder());
        pipeline.addLast("request-handler", new WebSocketServerProtocolHandler("/gremlin"));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));

        pipeline.addLast("response-encoder", new GremlinResponseEncoder());
        pipeline.addLast("request-text-decoder", new GremlinTextRequestDecoder());
        pipeline.addLast("request-binary-decoder", new GremlinBinaryRequestDecoder(serializers));

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-aggregator-encoder", LogLevel.DEBUG));
    }
}