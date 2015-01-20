package com.tinkerpop.gremlin.driver.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketGremlinRequestEncoder extends MessageToMessageEncoder<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketGremlinRequestEncoder.class);
    private boolean binaryEncoding = false;

    private final MessageSerializer serializer;

    public WebSocketGremlinRequestEncoder(final boolean binaryEncoding, final MessageSerializer serializer) {
        this.binaryEncoding = binaryEncoding;
        this.serializer = serializer;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessage requestMessage, final List<Object> objects) throws Exception {
        try {
            if (binaryEncoding) {
                final ByteBuf encodedMessage = serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc());
                objects.add(new BinaryWebSocketFrame(encodedMessage));
            } else {
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                objects.add(new TextWebSocketFrame(textSerializer.serializeRequestAsString(requestMessage)));
            }
        } catch (Exception ex) {
            logger.warn(String.format("An error occurred during serialization of this request [%s] - it could not be sent to the server.", requestMessage), ex);
        }
    }
}
