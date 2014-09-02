package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.driver.ser.JsonMessageSerializerV1d0;
import com.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpGremlinEndpointHandler extends ChannelInboundHandlerAdapter {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Map<String, MessageSerializer> serializers;
    private final MessageTextSerializer jsonSerializer = new JsonMessageSerializerV1d0();

    private final GremlinExecutor gremlinExecutor;

    public HttpGremlinEndpointHandler(final Map<String, MessageSerializer> serializers,
                                      final GremlinExecutor gremlinExecutor) {
        this.serializers = serializers;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (msg instanceof HttpRequest) {
            final HttpRequest req = (HttpRequest) msg;

            if (is100ContinueExpected(req)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }

            if (req.getMethod() != GET) {
                sendError(ctx, METHOD_NOT_ALLOWED);
                return;
            }

            final QueryStringDecoder decoder = new QueryStringDecoder(req.getUri());
            final String script = decoder.parameters().get("gremlin").get(0);

            final MessageTextSerializer serializer = (MessageTextSerializer) serializers.getOrDefault("application/json", jsonSerializer);

            try {
                final ResponseMessage responseMessage = ResponseMessage.build(UUID.randomUUID())
                        .code(ResponseStatusCode.SUCCESS)
                        .result(gremlinExecutor.eval(script).get()).create();

                final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(
                        serializer.serializeResponseAsString(responseMessage).getBytes(UTF8)));
                response.headers().set(CONTENT_TYPE, "application/json");
                response.headers().set(CONTENT_LENGTH, response.content().readableBytes());

                // handle cors business
                final String origin = req.headers().get(ORIGIN);
                if (origin != null)
                    response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            
                if (!isKeepAlive(req)) {
                    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                    ctx.write(response);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status) {
        final FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
