package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.SerializationException;
import com.tinkerpop.gremlin.driver.ser.Serializers;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Decodes the contents of a {@code BinaryWebSocketFrame}.  Binary-based frames assume that the format is encoded
 * in the first initial bytes of the message.  From there the proper serializer can be chosen and the message
 * can then be deserialized.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WsGremlinBinaryRequestDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {
    private static final Logger logger = LoggerFactory.getLogger(WsGremlinBinaryRequestDecoder.class);

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final Map<String, MessageSerializer> serializers;

    public WsGremlinBinaryRequestDecoder(final Map<String, MessageSerializer> serializers) {
        this.serializers = serializers;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final BinaryWebSocketFrame frame, final List<Object> objects) throws Exception {
        final ByteBuf messageBytes = frame.content();
        final byte len = messageBytes.readByte();
        if (len <= 0) {
            objects.add(RequestMessage.INVALID);
            return;
        }

        final ByteBuf contentTypeBytes = channelHandlerContext.alloc().buffer(len);
        try {
            messageBytes.readBytes(contentTypeBytes);
            final String contentType = contentTypeBytes.toString(UTF8);
            final MessageSerializer serializer = select(contentType, Serializers.DEFAULT_REQUEST_SERIALIZER);

            channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
            channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(true);
            try {
                objects.add(serializer.deserializeRequest(messageBytes.discardReadBytes()));
            } catch (SerializationException se) {
                objects.add(RequestMessage.INVALID);
            }
        } finally {
            contentTypeBytes.release();
        }
    }

    public MessageSerializer select(final String mimeType, final MessageSerializer defaultSerializer) {
        if (logger.isWarnEnabled() && !serializers.containsKey(mimeType))
            logger.warn("Gremlin Server is not configured with a serializer for the requested mime type [{}] - using {} by default",
                    mimeType, defaultSerializer.getClass().getName());

        return serializers.getOrDefault(mimeType, defaultSerializer);
    }
}
