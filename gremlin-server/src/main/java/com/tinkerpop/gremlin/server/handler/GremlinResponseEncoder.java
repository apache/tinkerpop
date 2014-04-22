package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinResponseEncoder extends MessageToMessageEncoder<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseEncoder.class);

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final ResponseMessage o, final List<Object> objects) throws Exception {
        final MessageSerializer serializer = MessageSerializer.select(
                o.getContentType(),
                MessageSerializer.DEFAULT_RESULT_SERIALIZER);

        try {
            if (o.getCode().isSuccess())
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResponseAsString(o)));
            else {
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResponseAsString(o)));
                final ResponseMessage terminator = ResponseMessage.create(o.getRequestId(), o.getContentType()).code(ResultCode.SUCCESS_TERMINATOR).build();
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResponseAsString(terminator)));
            }

        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", o.getResult(), o.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s",
                    ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
            final ResponseMessage error = ResponseMessage.create(o.getRequestId(), o.getContentType())
                    .result(errorMessage)
                    .code(ResultCode.SERVER_ERROR_SERIALIZATION).build();
            channelHandlerContext.write(new TextWebSocketFrame(serializer.serializeResponseAsString(error)));
            final ResponseMessage terminator = ResponseMessage.create(o.getRequestId(), o.getContentType()).code(ResultCode.SUCCESS_TERMINATOR).build();
            channelHandlerContext.writeAndFlush(new TextWebSocketFrame(serializer.serializeResponseAsString(terminator)));
        }
    }
}
