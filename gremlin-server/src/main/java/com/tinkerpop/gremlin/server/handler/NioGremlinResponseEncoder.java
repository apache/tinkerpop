package com.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioGremlinResponseEncoder extends MessageToByteEncoder<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(NioGremlinResponseEncoder.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final ResponseMessage responseMessage, final ByteBuf byteBuf) throws Exception {
        final MessageSerializer serializer = channelHandlerContext.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = channelHandlerContext.channel().attr(StateKey.USE_BINARY).get();

        try {
            if (useBinary) {
                if (responseMessage.getStatus().getCode().isSuccess())
                    byteBuf.writeBytes(serializer.serializeResponseAsBinary(responseMessage, channelHandlerContext.alloc()));
                else {
                    byteBuf.writeBytes(serializer.serializeResponseAsBinary(responseMessage, channelHandlerContext.alloc()));
                    final ResponseMessage terminator = ResponseMessage.build(responseMessage.getRequestId()).code(ResponseStatusCode.SUCCESS_TERMINATOR).create();
                    byteBuf.writeBytes(serializer.serializeResponseAsBinary(terminator, channelHandlerContext.alloc()));
                    errorMeter.mark();
                }
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                if (responseMessage.getStatus().getCode().isSuccess())
                    byteBuf.writeBytes(textSerializer.serializeResponseAsString(responseMessage).getBytes(UTF8));
                else {
                    byteBuf.writeBytes(textSerializer.serializeResponseAsString(responseMessage).getBytes(UTF8));
                    final ResponseMessage terminator = ResponseMessage.build(responseMessage.getRequestId()).code(ResponseStatusCode.SUCCESS_TERMINATOR).create();
                    byteBuf.writeBytes(textSerializer.serializeResponseAsString(terminator).getBytes(UTF8));
                    errorMeter.mark();
                }
            }
        } catch (Exception ex) {
            errorMeter.mark();
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", responseMessage.getResult(), responseMessage.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s",
                    ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
            final ResponseMessage error = ResponseMessage.build(responseMessage.getRequestId())
                    .statusMessage(errorMessage)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            if (useBinary) {
                channelHandlerContext.write(serializer.serializeResponseAsBinary(error, channelHandlerContext.alloc()));
                final ResponseMessage terminator = ResponseMessage.build(responseMessage.getRequestId()).code(ResponseStatusCode.SUCCESS_TERMINATOR).create();
                channelHandlerContext.writeAndFlush(serializer.serializeResponseAsBinary(terminator, channelHandlerContext.alloc()));
            } else {
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                channelHandlerContext.write(textSerializer.serializeResponseAsString(error));
                final ResponseMessage terminator = ResponseMessage.build(responseMessage.getRequestId()).code(ResponseStatusCode.SUCCESS_TERMINATOR).create();
                channelHandlerContext.writeAndFlush(textSerializer.serializeResponseAsString(terminator));
            }
        }
    }
}
