package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.javatuples.Pair;

import java.util.List;

/**
 * Decodes the contents of a {@link TextWebSocketFrame}.  Text-based frames are always assumed to be
 * "application/json" when it comes to serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinTextRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        // todo: use the channel to store the serializer until this is proven wrong
        channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(MessageSerializer.DEFAULT_REQUEST_SERIALIZER);
        channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(true);
        objects.add(MessageSerializer.DEFAULT_REQUEST_SERIALIZER.deserializeRequest(frame.text()).orElse(RequestMessage.INVALID));
    }
}
