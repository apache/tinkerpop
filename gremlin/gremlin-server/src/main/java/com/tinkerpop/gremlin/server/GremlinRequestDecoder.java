package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        // message consists of two parts.  the first part has the mime type of the incoming message and the
        // second part is the message itself.  these two parts are separated by a "|-". if there aren't two parts
        // assume application/json and that the entire message is that format (i.e. there is no "mimetype|-")
        final String[] parts = segmentMessage(frame.text());
        objects.add(MessageSerializer.select(parts[0], MessageSerializer.DEFAULT_REQUEST_SERIALIZER)
                .deserializeRequest(parts[1]).orElse(RequestMessage.INVALID));
    }

    private static String[] segmentMessage(final String msg) {
        final int splitter = msg.indexOf("|-");
        if (splitter == -1)
            return new String[] {"application/json", msg};

        return new String[] {msg.substring(0, splitter), msg.substring(splitter + 2)};
    }
}
