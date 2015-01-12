package com.tinkerpop.gremlin.driver.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioGremlinRequestEncoder extends MessageToByteEncoder<Object> {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketGremlinRequestEncoder.class);
    private boolean binaryEncoding = false;

    private final MessageSerializer serializer;

    public NioGremlinRequestEncoder(final boolean binaryEncoding, final MessageSerializer serializer) {
        this.binaryEncoding = binaryEncoding;
        this.serializer = serializer;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final Object msg, final ByteBuf byteBuf) throws Exception {
        final RequestMessage requestMessage = (RequestMessage) msg;
        try {
            if (binaryEncoding) {
                byteBuf.writeBytes(serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc()));
            } else {
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                byteBuf.writeBytes(textSerializer.serializeRequestAsString(requestMessage).getBytes(CharsetUtil.UTF_8));
            }
        } catch (Exception ex) {
            logger.warn(String.format("An error occurred during serialization of this request [%s] - it could not be sent to the server.", requestMessage), ex);
        }
    }
}
