/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents the HTTP request that will be sent to the server. It is passed through the
 * {@link RequestInterceptor} chain where interceptors can modify headers, body, URI, and method.
 * <p>
 * The body starts as a {@link RequestMessage} and can be serialized to JSON bytes via {@link #serializeBody()}.
 * After all interceptors run, if the body is still a {@code RequestMessage}, the driver will call
 * {@code serializeBody()} automatically before sending.
 */
public class HttpRequest {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static class Headers {
        // Add as needed. Headers are case-insensitive; lower case for now to match Netty.
        public static final String ACCEPT = "accept";
        public static final String ACCEPT_ENCODING = "accept-encoding";
        public static final String AUTHORIZATION = "authorization";
        public static final String CONTENT_TYPE = "content-type";
        public static final String CONTENT_LENGTH = "content-length";
        public static final String DEFLATE = "deflate";
        public static final String HOST = "host";
        public static final String USER_AGENT = "user-agent";
    }

    public static class Method {
        public static final String GET = "GET";
        public static final String POST = "POST";
    }

    private final Map<String, String> headers;
    private Object body;
    private URI uri;
    private String method;

    /**
     * Constructor that defaults the method to {@code POST}.
     */
    public HttpRequest(final Map<String, String> headers, final Object body, final URI uri) {
        this(headers, body, uri, Method.POST);
    }

    /**
     * Full constructor.
     */
    public HttpRequest(final Map<String, String> headers, final Object body, final URI uri, final String method) {
        this.headers = headers;
        this.body = body;
        this.uri = uri;
        this.method = method;
    }

    /**
     * Get the headers of the request.
     *
     * @return a map of headers. This can be used to directly update the entries.
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Get the body of the request.
     *
     * @return an Object representing the body ({@link RequestMessage} or {@code byte[]}).
     */
    public Object getBody() {
        return body;
    }

    /**
     * Get the URI of the request.
     *
     * @return the request URI.
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Get the HTTP method of the request. The standard {@code /gremlin} endpoint only supports {@code POST}.
     *
     * @return the HTTP method.
     */
    public String getMethod() {
        return method;
    }

    /**
     * Set the HTTP body of the request.
     *
     * @return this HttpRequest for method chaining.
     */
    public HttpRequest setBody(final Object body) {
        this.body = body;
        return this;
    }

    /**
     * Set the HTTP method of the request.
     *
     * @return this HttpRequest for method chaining.
     */
    public HttpRequest setMethod(final String method) {
        this.method = method;
        return this;
    }

    /**
     * Set the URI of the request.
     *
     * @return this HttpRequest for method chaining.
     */
    public HttpRequest setUri(final URI uri) {
        this.uri = uri;
        return this;
    }

    /**
     * Serialize the body to JSON bytes if it is still a {@link RequestMessage}. If the body is already
     * {@code byte[]}, this method is idempotent and returns the existing bytes. This method also sets the
     * {@code Content-Type} header to {@code application/json} and the {@code Content-Length} header to the
     * byte length of the serialized body.
     * <p>
     * Interceptors that need the serialized payload (e.g., for computing a signature hash) should call
     * this method rather than serializing independently.
     *
     * @return the serialized body bytes
     * @throws IllegalStateException if the body is neither a {@link RequestMessage} nor {@code byte[]}
     */
    public byte[] serializeBody() {
        if (body instanceof byte[]) {
            return (byte[]) body;
        }

        if (!(body instanceof RequestMessage)) {
            throw new IllegalStateException("Cannot serialize body of type " +
                    (body == null ? "null" : body.getClass().getSimpleName()) +
                    ". Expected RequestMessage or byte[].");
        }

        final RequestMessage requestMessage = (RequestMessage) body;

        // Build JSON map: gremlin is top-level, plus all fields from the message
        final Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("gremlin", requestMessage.getGremlin());
        jsonMap.putAll(requestMessage.getFields());

        try {
            final byte[] jsonBytes = mapper.writeValueAsBytes(jsonMap);
            this.body = jsonBytes;
            this.headers.put(Headers.CONTENT_TYPE, "application/json");
            this.headers.put(Headers.CONTENT_LENGTH, String.valueOf(jsonBytes.length));
            return jsonBytes;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize RequestMessage to JSON", e);
        }
    }
}
