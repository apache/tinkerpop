package com.tinkerpop.gremlin.driver.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioGremlinResponseDecoder extends ByteToMessageDecoder {
    private final MessageSerializer serializer;

    public NioGremlinResponseDecoder(final MessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf byteBuf, final List<Object> objects) throws Exception {
        if (byteBuf.readableBytes() < 1) return;
        objects.add(serializer.deserializeResponse(byteBuf));
    }
}
