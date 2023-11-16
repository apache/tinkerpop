/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Meter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Provides methods shared by the HTTP handlers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpHandlerUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpHandlerUtil.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));
    private static final String ARGS_BINDINGS_DOT = Tokens.ARGS_BINDINGS + ".";
    private static final String ARGS_ALIASES_DOT = Tokens.ARGS_ALIASES + ".";
    /**
     * This is just a generic mapper to interpret the JSON of a POSTed request.  It is not used for the serialization
     * of the response.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Convert a http request into a {@link RequestMessage}.
     * There are 2 payload types options here.
     * 1.
     *     existing https://tinkerpop.apache.org/docs/current/reference/#connecting-via-http
     *     intended to use with curl, postman, etc. by users
     *     both GET and POST
     *     Content-Type header can be empty or application/json
     *     Accept header can be any, most useful can be application/json, text/plain, application/vnd.gremlin-v3.0+json and application/vnd.gremlin-v3.0+json;types=false
     *     Request body example: { "gremlin": "g.V()" }
     * 2.
     *     experimental payload with serialized RequestMessage
     *     intended for drivers/GLV's. Support both gremlin and bytecode queries.
     *     only POST
     *     Content-Type is defined by used serializer, expected type GraphSON application/vnd.gremlin-v3.0+json or GraphBinary application/vnd.graphbinary-v1.0. Untyped GraphSON is not supported, it can't deserialize bytecode
     *     Accept header can be any.
     *     Request body contains serialized RequestMessage
     */
    public static RequestMessage getRequestMessageFromHttpRequest(final FullHttpRequest request,
                                                                  Map<String, MessageSerializer<?>> serializers) throws SerializationException {
        final String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);

        if (request.method() == POST && contentType != null && !contentType.equals("application/json") && serializers.containsKey(contentType)) {
            final MessageSerializer<?> serializer = serializers.get(contentType);

            final ByteBuf buffer = request.content();

            // additional validation for header
            final int first = buffer.readByte();
            // payload can be plain json or can start with additional header with content type.
            // if first character is not "{" (0x7b) then need to verify is correct serializer selected.
            if (first != 0x7b) {
                final byte[] bytes = new byte[first];
                buffer.readBytes(bytes);
                final String mimeType = new String(bytes, StandardCharsets.UTF_8);

                if (Arrays.stream(serializer.mimeTypesSupported()).noneMatch(t -> t.equals(mimeType)))
                    throw new IllegalArgumentException("Mime type mismatch. Value in content-type header is not equal payload header.");
            } else
                buffer.resetReaderIndex();

            return serializer.deserializeRequest(buffer);
        }

        return getRequestMessageFromHttpRequest(request);
    }

    /**
     * Convert a http request into a {@link RequestMessage}.
     */
    public static RequestMessage getRequestMessageFromHttpRequest(final FullHttpRequest request) {
        // default is just the StandardOpProcessor which maintains compatibility with older versions which only
        // processed scripts.
        final RequestMessage.Builder msgBuilder = RequestMessage.build(StandardOpProcessor.OP_PROCESSOR_NAME);

        if (request.method() == GET) {
            final QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
            final List<String> gremlinParms = decoder.parameters().get(Tokens.ARGS_GREMLIN);

            if (null == gremlinParms || gremlinParms.size() == 0)
                throw new IllegalArgumentException("no gremlin script supplied");
            final String script = gremlinParms.get(0);
            if (script.isEmpty()) throw new IllegalArgumentException("no gremlin script supplied");

            final List<String> requestIdParms = decoder.parameters().get(Tokens.REQUEST_ID);
            if (requestIdParms != null && requestIdParms.size() > 0) {
                msgBuilder.overrideRequestId(UUID.fromString(requestIdParms.get(0)));
            }

            // query string parameters - take the first instance of a key only - ignore the rest
            final Map<String, Object> bindings = new HashMap<>();
            decoder.parameters().entrySet().stream().filter(kv -> kv.getKey().startsWith(ARGS_BINDINGS_DOT))
                    .forEach(kv -> bindings.put(kv.getKey().substring(ARGS_BINDINGS_DOT.length()), kv.getValue().get(0)));

            final Map<String, String> aliases = new HashMap<>();
            decoder.parameters().entrySet().stream().filter(kv -> kv.getKey().startsWith(ARGS_ALIASES_DOT))
                    .forEach(kv -> aliases.put(kv.getKey().substring(ARGS_ALIASES_DOT.length()), kv.getValue().get(0)));

            final List<String> languageParms = decoder.parameters().get(Tokens.ARGS_LANGUAGE);
            final String language = (null == languageParms || languageParms.size() == 0) ? null : languageParms.get(0);

            return msgBuilder.addArg(Tokens.ARGS_GREMLIN, script).addArg(Tokens.ARGS_LANGUAGE, language)
                    .addArg(Tokens.ARGS_BINDINGS, bindings).addArg(Tokens.ARGS_ALIASES, aliases).create();
        } else {
            final JsonNode body;
            try {
                body = mapper.readTree(request.content().toString(CharsetUtil.UTF_8));
            } catch (IOException ioe) {
                throw new IllegalArgumentException("body could not be parsed", ioe);
            }

            final JsonNode scriptNode = body.get(Tokens.ARGS_GREMLIN);
            if (null == scriptNode) throw new IllegalArgumentException("no gremlin script supplied");

            final JsonNode bindingsNode = body.get(Tokens.ARGS_BINDINGS);
            if (bindingsNode != null && !bindingsNode.isObject())
                throw new IllegalArgumentException("bindings must be a Map");

            final Map<String, Object> bindings = new HashMap<>();
            if (bindingsNode != null)
                bindingsNode.fields().forEachRemaining(kv -> bindings.put(kv.getKey(), fromJsonNode(kv.getValue())));

            final JsonNode aliasesNode = body.get(Tokens.ARGS_ALIASES);
            if (aliasesNode != null && !aliasesNode.isObject())
                throw new IllegalArgumentException("aliases must be a Map");

            final Map<String, String> aliases = new HashMap<>();
            if (aliasesNode != null)
                aliasesNode.fields().forEachRemaining(kv -> aliases.put(kv.getKey(), kv.getValue().asText()));

            final JsonNode languageNode = body.get(Tokens.ARGS_LANGUAGE);
            final String language = null == languageNode ? null : languageNode.asText();

            final JsonNode requestIdNode = body.get(Tokens.REQUEST_ID);
            final UUID requestId = null == requestIdNode ? UUID.randomUUID() : UUID.fromString(requestIdNode.asText());

            final JsonNode opNode = body.get("op");
            final String op = null == opNode ? "" : opNode.asText();

            return msgBuilder.overrideRequestId(requestId).processor(op)
                    .addArg(Tokens.ARGS_GREMLIN, scriptNode.asText()).addArg(Tokens.ARGS_LANGUAGE, language)
                    .addArg(Tokens.ARGS_BINDINGS, bindings).addArg(Tokens.ARGS_ALIASES, aliases).create();
        }
    }

    private static Object fromJsonNode(final JsonNode node) {
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

    static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status,
                          final String message, final boolean keepAlive) {
        sendError(ctx, status, message, Optional.empty(), keepAlive);
    }

    static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status,
                          final String message, final Optional<Throwable> t, final boolean keepAlive) {
        if (t.isPresent())
            logger.warn(String.format("Invalid request - responding with %s and %s", status, message), t.get());
        else
            logger.warn(String.format("Invalid request - responding with %s and %s", status, message));

        errorMeter.mark();
        final ObjectNode node = mapper.createObjectNode();
        node.put("message", message);
        if (t.isPresent()) {
            // "Exception-Class" needs to go away - didn't realize it was named that way during review for some reason.
            // replaced with the same method for exception reporting as is used with websocket/nio protocol
            node.put("Exception-Class", t.get().getClass().getName());
            final ArrayNode exceptionList = node.putArray(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS);
            ExceptionUtils.getThrowableList(t.get()).forEach(throwable -> exceptionList.add(throwable.getClass().getName()));
            node.put(Tokens.STATUS_ATTRIBUTE_STACK_TRACE, ExceptionUtils.getStackTrace(t.get()));
        }

        final FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer(node.toString(), CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "application/json");

        sendAndCleanupConnection(ctx, keepAlive, response);
    }

    static void sendAndCleanupConnection(final ChannelHandlerContext ctx,
                                         final boolean keepAlive,
                                         final FullHttpResponse response) {
        HttpUtil.setKeepAlive(response, keepAlive);
        HttpUtil.setContentLength(response, response.content().readableBytes());

        final ChannelFuture flushPromise = ctx.writeAndFlush(response);

        if (!keepAlive) {
            // Close the connection as soon as the response is sent.
            flushPromise.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
