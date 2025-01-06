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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.server.util.TextPlainMessageSerializer;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.sendError;

/**
 * Decodes the contents of a {@link FullHttpRequest}. This will extract the {@link RequestMessage} from the
 * {@link FullHttpRequest} or, if unsuccessful, will flush an error back.
 */
@ChannelHandler.Sharable
public class HttpRequestMessageDecoder extends MessageToMessageDecoder<FullHttpRequest> {
    private static final Pattern pattern = Pattern.compile("(.*);q=(.*)");

    /**
     * Serializer for {@code text/plain} which is a serializer exclusive to HTTP.
     */
    private final TextPlainMessageSerializer textPlainSerializer = new TextPlainMessageSerializer();

    private final Map<String, MessageSerializer<?>> serializers;

    /**
     * A generic mapper to decode an application/json request.
     */
    private final ObjectMapper mapper = new ObjectMapper();


    public HttpRequestMessageDecoder(final Map<String, MessageSerializer<?>> serializers) {
        this.serializers = serializers;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final FullHttpRequest req, final List<Object> objects) throws Exception {
        ctx.channel().attr(StateKey.REQUEST_HEADERS).set(req.headers());

        final String acceptMime = Optional.ofNullable(req.headers().get(HttpHeaderNames.ACCEPT)).orElse("application/json");
        final Pair<String, MessageSerializer<?>> serializer = chooseSerializer(acceptMime);

        if (req.method() != POST) {
            sendError(ctx, METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED.toString());
            return;
        }

        if (null == serializer) {
            sendError(ctx, BAD_REQUEST, String.format("no serializer for requested Accept header: %s", acceptMime));
            return;
        }

        final RequestMessage requestMessage;
        try {
            requestMessage = getRequestMessageFromHttpRequest(req, serializers);
        } catch (IllegalArgumentException | SerializationException | NullPointerException ex) {
            sendError(ctx, BAD_REQUEST, ex.getMessage());
            return;
        }

        // checked in getRequestMessageFromHttpRequest
        ctx.channel().attr(StateKey.SERIALIZER).set(Pair.with(serializer.getValue0(), serializer.getValue1()));
        objects.add(requestMessage);
    }

    private Pair<String, MessageSerializer<?>> chooseSerializer(final String mimeType) {
        final List<Pair<String, Double>> ordered = Stream.of(mimeType.split(",")).map(mediaType -> {
            // parse out each mediaType with its params - keeping it simple and just looking for "quality".  if
            // that value isn't there, default it to 1.0.  not really validating here so users better get their
            // accept headers straight
            final Matcher matcher = pattern.matcher(mediaType);
            return (matcher.matches()) ? Pair.with(matcher.group(1), Double.parseDouble(matcher.group(2))) : Pair.with(mediaType, 1.0);
        }).sorted((o1, o2) -> o2.getValue0().compareTo(o1.getValue0())).collect(Collectors.toList());

        for (Pair<String, Double> p : ordered) {
            // this isn't perfect as it doesn't really account for wildcards.  that level of complexity doesn't seem
            // super useful for gremlin server really.
            final String accept = p.getValue0().equals("*/*") ? "application/json" : p.getValue0();
            if (serializers.containsKey(accept))
                return Pair.with(accept, serializers.get(accept));
            else if (accept.equals("text/plain")) {
                return Pair.with(accept, textPlainSerializer);
            }
        }

        return null;
    }

    /**
     * Convert a http request into a {@link RequestMessage}.
     * There are 2 payload types options here.
     * 1.
     *     existing https://tinkerpop.apache.org/docs/current/reference/#connecting-via-http
     *     intended to use with curl, postman, etc. by users
     *     both GET and POST
     *     Content-Type header can be empty or application/json
     *     Accept header can be any, most useful can be application/json, text/plain, application/vnd.gremlin-v3.0+json
     *     and application/vnd.gremlin-v3.0+json;types=false
     *     Request body example: { "gremlin": "g.V()" }
     * 2.
     *     experimental payload with serialized RequestMessage
     *     intended for drivers/GLV's. Support only gremlin.
     *     only POST
     *     Content-Type is defined by used serializer, expected type GraphSON application/vnd.gremlin-v3.0+json or
     *     GraphBinary application/vnd.graphbinary-v1.0. Untyped GraphSON is not supported, it can't deserialize parameters
     *     Accept header can be any.
     *     Request body contains serialized RequestMessage
     */
    public RequestMessage getRequestMessageFromHttpRequest(final FullHttpRequest request,
                                                           Map<String, MessageSerializer<?>> serializers) throws SerializationException {
        final String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);

        if (contentType != null && !contentType.equals("application/json") && serializers.containsKey(contentType)) {
            final MessageSerializer<?> serializer = serializers.get(contentType);
            final ByteBuf buffer = request.content();

            try {
                return serializer.deserializeBinaryRequest(buffer);
            } catch (Exception e) {
                throw new SerializationException("Unable to deserialize request using: " + serializer.getClass().getSimpleName(), e);
            }
        }
        return getRequestMessageFromHttpRequest(request);
    }

    private RequestMessage getRequestMessageFromHttpRequest(final FullHttpRequest request) {
        final JsonNode body;
        try {
            body = mapper.readTree(request.content().toString(CharsetUtil.UTF_8));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("body could not be parsed", ioe);
        }

        final JsonNode scriptNode = body.get(Tokens.ARGS_GREMLIN);
        if (null == scriptNode) throw new IllegalArgumentException("no gremlin script supplied");

        final RequestMessage.Builder builder = RequestMessage.build(scriptNode.asText());

        final JsonNode bindingsNode = body.get(Tokens.ARGS_BINDINGS);
        if (bindingsNode != null && !bindingsNode.isObject())
            throw new IllegalArgumentException("bindings must be a Map");

        final Map<String, Object> bindings = new HashMap<>();
        if (bindingsNode != null)
            bindingsNode.fields().forEachRemaining(kv -> bindings.put(kv.getKey(), fromJsonNode(kv.getValue())));
        builder.addBindings(bindings);

        final JsonNode gNode = body.get(Tokens.ARGS_G);
        if (null != gNode) builder.addG(gNode.asText());

        final JsonNode languageNode = body.get(Tokens.ARGS_LANGUAGE);
        builder.addLanguage((null == languageNode) ? "gremlin-lang" : languageNode.asText());

        final JsonNode chunkSizeNode = body.get(Tokens.ARGS_BATCH_SIZE);
        if (null != chunkSizeNode) builder.addChunkSize(chunkSizeNode.asInt());

        final JsonNode timeoutMsNode = body.get(Tokens.TIMEOUT_MS);
        if (null != timeoutMsNode) builder.addTimeoutMillis(timeoutMsNode.asLong());

        final JsonNode matPropsNode = body.get(Tokens.ARGS_MATERIALIZE_PROPERTIES);
        if (null != matPropsNode) builder.addMaterializeProperties(matPropsNode.asText());

        return builder.create();
    }

    private Object fromJsonNode(final JsonNode node) {
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
}
