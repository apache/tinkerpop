package com.tinkerpop.gremlin.driver.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class WebSocketGremlinResponseDecoder extends MessageToMessageDecoder<WebSocketFrame> {
    private final MessageSerializer serializer;

    public WebSocketGremlinResponseDecoder(final MessageSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final WebSocketFrame webSocketFrame, final List<Object> objects) throws Exception {
        try {
            if (webSocketFrame instanceof BinaryWebSocketFrame) {
                final BinaryWebSocketFrame tf = (BinaryWebSocketFrame) webSocketFrame;
                objects.add(serializer.deserializeResponse(tf.content()));
            } else {
                final TextWebSocketFrame tf = (TextWebSocketFrame) webSocketFrame;
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                objects.add(textSerializer.deserializeResponse(tf.text()));
            }
        } finally {
            ReferenceCountUtil.release(webSocketFrame);
        }
    }
}
