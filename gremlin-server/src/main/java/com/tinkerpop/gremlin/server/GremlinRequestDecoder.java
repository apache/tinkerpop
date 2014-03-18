package com.tinkerpop.gremlin.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.javatuples.Pair;

import java.util.List;

/**
 * Decodes the contents of a {@link TextWebSocketFrame}.  The frame contains text where there is either a delimited
 * string, separated by a "|-".  The part before that delimiter is the mime type of the message that follows the
 * delimiter.  If no delimiter is present, the decoder assumes "application/json".
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinRequestDecoder extends MessageToMessageDecoder<TextWebSocketFrame> {
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final TextWebSocketFrame frame, final List<Object> objects) throws Exception {
        final Pair<String, String> parts = segmentMessage((frame).text());

        // if the message cannot be deserialized it is passed through as an invalid message
        objects.add(MessageSerializer.select(parts.getValue0(), MessageSerializer.DEFAULT_REQUEST_SERIALIZER)
                                     .deserializeRequest(parts.getValue1()).orElse(RequestMessage.INVALID));
    }

    private static Pair<String, String> segmentMessage(final String msg) {
        final int splitter = msg.indexOf("|-");
        if (splitter == -1)
            return Pair.with("application/json", msg);

        return Pair.with(msg.substring(0, splitter), msg.substring(splitter + 2));
    }
}
