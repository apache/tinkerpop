package com.tinkerpop.gremlin.server.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    private static final String KEY_GREMLIN = "gremlin";

    private final Map<String, MessageSerializer> serializers;
    private final MessageTextSerializer jsonSerializer = new JsonMessageSerializerV1d0();

    private static final ObjectMapper mapper = new ObjectMapper();

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
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest req = (FullHttpRequest) msg;

            if (is100ContinueExpected(req)) {
                ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
            }

            if (req.getMethod() != GET && req.getMethod() != POST) {
                sendError(ctx, METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED.toString());
                return;
            }

            final Pair<String, Map<String,Object>> scriptAndBindings;
            try {
                scriptAndBindings = getGremlinScript(req);
            } catch (IllegalArgumentException iae) {
                sendError(ctx, BAD_REQUEST, iae.getMessage());
                return;
            }

            final MessageTextSerializer serializer = (MessageTextSerializer) serializers.getOrDefault("application/json", jsonSerializer);

            try {
                final ResponseMessage responseMessage = ResponseMessage.build(UUID.randomUUID())
                        .code(ResponseStatusCode.SUCCESS)
                        .result(gremlinExecutor.eval(scriptAndBindings.getValue0(), scriptAndBindings.getValue1()).get()).create();

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

    private static Pair<String, Map<String,Object>> getGremlinScript(final FullHttpRequest request) {
        if (request.getMethod() == GET) {
            final QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
            final List<String> gremlinParms = decoder.parameters().get(KEY_GREMLIN);
            if (gremlinParms == null || gremlinParms.size() == 0)
                throw new IllegalArgumentException("no gremlin script supplied");
            final String script = gremlinParms.get(0);
            if (script.isEmpty()) throw new IllegalArgumentException("no gremlin script supplied");

            // query string parameters - take the first instance of a key only - ignore the rest
            final Map<String,Object> bindings = new HashMap<>();
            decoder.parameters().entrySet().stream().filter(kv -> !kv.getKey().equals(KEY_GREMLIN))
                    .forEach(kv -> bindings.put(kv.getKey(), kv.getValue().get(0)));

            return Pair.with(script, bindings);
        } else {
            final JsonNode body;
            try {
                body = mapper.readTree(request.content().toString(CharsetUtil.UTF_8));
            } catch (IOException ioe) {
                throw new IllegalArgumentException("body could not be parsed", ioe);
            }

            final JsonNode scriptNode = body.get(KEY_GREMLIN);
            if (null == scriptNode) throw new IllegalArgumentException("no gremlin script supplied");

            final JsonNode bindingsNode = body.get("bindings");
            if (bindingsNode != null && !bindingsNode.isObject()) throw new IllegalArgumentException("bindings must be a Map");

            final Map<String,Object> bindings = new HashMap<>();
            if (bindingsNode != null)
                bindingsNode.fields().forEachRemaining(kv -> bindings.put(kv.getKey(), fromJsonNode(kv.getValue())));

            return Pair.with(scriptNode.asText(), bindings);
        }
    }

    public static Object fromJsonNode(final JsonNode node) {
        if (node.isNull())
            return null;
        else if (node.isObject()) {
            final Map<String, Object> map = new HashMap<>();
            final ObjectNode objectNode = (ObjectNode) node;
            final Iterator<String> iterator = objectNode.fieldNames();
            while (iterator.hasNext()) {
                String key = iterator.next();
                map.put(key, fromJsonNode(objectNode.get(key)));
            }
            return map;
        } else if (node.isArray()) {
            final ArrayNode arrayNode = (ArrayNode) node;
            final ArrayList<Object> array = new ArrayList<>();
            for (int i = 0; i < arrayNode.size(); i++) {
                array.add(fromJsonNode(arrayNode.get(i)));
            }
            return array;
        } else if (node.isFloatingPointNumber())
            return node.asDouble();
        else if (node.isIntegralNumber())
            return node.asLong();
        else if (node.isBoolean())
            return node.asBoolean();
        else
            return node.asText();
    }

    private static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status, final String message) {
        final FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + message + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
