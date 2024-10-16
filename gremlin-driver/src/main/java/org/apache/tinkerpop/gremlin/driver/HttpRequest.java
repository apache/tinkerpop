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
package org.apache.tinkerpop.gremlin.driver;

import java.net.URI;
import java.util.Map;

/**
 * HttpRequest represents the data that will be used to create the actual request to the remote endpoint. It will be
 * passed to different {@link RequestInterceptor} that can update its values. The body can be anything as the
 * interceptor may change what the payload is. Also contains some convenience Strings for common HTTP header key and
 * values and HTTP methods.
 */
public class HttpRequest {
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
     * @return an Object representing the body.
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
     * Set the HTTP body of the request. During processing, the body can be any type but the final interceptor must set
     * the body to a {@code byte[]}.
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
}
