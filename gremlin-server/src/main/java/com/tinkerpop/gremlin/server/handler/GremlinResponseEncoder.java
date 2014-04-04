package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.server.MessageSerializer;
import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.Tokens;
import com.tinkerpop.gremlin.server.message.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinResponseEncoder extends MessageToMessageEncoder<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseEncoder.class);

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final ResponseMessage o, final List<Object> objects) throws Exception {
        final MessageSerializer serializer = MessageSerializer.select(
                o.getRequestMessage().<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"),
                MessageSerializer.DEFAULT_RESULT_SERIALIZER);

        try {
            if (o.getCode().isSuccess())
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResult(Optional.ofNullable(o.getResult()), o.getCode(), Optional.of(o.getRequestMessage()))));
            else {
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResult(Optional.ofNullable(o.getResult()), o.getCode(), Optional.of(o.getRequestMessage()))));
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResult(Optional.empty(), ResultCode.SUCCESS_TERMINATOR, Optional.of(o.getRequestMessage()))));
            }

        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", o.getResult(), o.getRequestMessage(), ex);
            final String errorMessage = String.format("Error during serialization: %s",
                    ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
            channelHandlerContext.write(new TextWebSocketFrame(serializer.serializeResult(Optional.<Object>ofNullable(errorMessage), ResultCode.SERVER_ERROR_SERIALIZATION, Optional.ofNullable(o.getRequestMessage()))));
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(serializer.serializeResult(Optional.empty(), ResultCode.SUCCESS_TERMINATOR, Optional.of(o.getRequestMessage()))));
        }
    }
}
