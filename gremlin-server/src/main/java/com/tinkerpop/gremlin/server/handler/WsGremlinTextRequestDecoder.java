package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import com.tinkerpop.gremlin.driver.ser.SerializationException;
import com.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;

/**
 * Decodes the contents of a {@link TextWebSocketFrame}.  Text-based frames are always assumed to be
 * "application/json" when it comes to serialization.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WsGremlinTextRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        // the default serializer must be a MessageTextSerializer instance to be compatible with this decoder
        final MessageTextSerializer serializer = (MessageTextSerializer) Serializers.DEFAULT_REQUEST_SERIALIZER;
        channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
        channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(false);

        try {
            objects.add(serializer.deserializeRequest(frame.text()));
        } catch (SerializationException se) {
            objects.add(RequestMessage.INVALID);
        }
    }
}
