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
package org.apache.tinkerpop.gremlin.util.message;

import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Response status codes for Gremlin Server responses. Result codes tend to map to
 * <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html">HTTP status codes</a>.  It is not a one-to-one
 * mapping and there are mapper status codes to be considered.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum ResponseStatusCode {

    /**
     * Code 200: The server successfully processed a request to completion - there are no messages remaining in this
     * stream.
     *
     * @since 3.0.0-incubating
     */
    SUCCESS(200),

    /**
     * Code 204: The server processed the request but there is no result to return (e.g. an {@link Iterator} with no
     * elements).
     *
     * @since 3.0.0-incubating
     */
    NO_CONTENT(204),

    /**
     * Code 206: The server successfully returned some content, but there is more in the stream to arrive - wait for a
     * {@link #SUCCESS} to signify the end of the stream.
     *
     * @since 3.0.0-incubating
     */
    PARTIAL_CONTENT(206),

    /**
     * The server cannot or will not process the request due to an apparent client error (e.g., malformed request
     * syntax, size too large, invalid request message framing, or deceptive request routing).
     *
     * @since 4.0.0
     */
    BAD_REQUEST(400),

    /**
     * Code 401: The server could not authenticate the request or the client requested a resource it did not have
     * access to.
     *
     * @since 3.0.0-incubating
     */
    UNAUTHORIZED(401),

    /**
     * Code 403: The server could authenticate the request, but will not fulfill it.  This is a general purpose code
     * that would typically be returned if the request is authenticated but not authorized to do what it is doing.
     *
     * @since 3.0.1-incubating
     */
    FORBIDDEN(403),

    /**
     * Code 407: A challenge from the server for the client to authenticate its request.
     *
     * @since 3.0.1-incubating
     */
    AUTHENTICATE(407),

    /**
     * The request is larger than the server is willing or able to process.
     *
     * @since 4.0.0
     */
    PAYLOAD_TOO_LARGE(413),

    /**
     * Code 429: Indicates that too many requests have been sent in a given amount of time.
     *
     * @since 3.5.0
     */
    TOO_MANY_REQUESTS(429),

    /**
     * Code 497: The request message contains objects that were not serializable on the client side.
     *
     * @since 3.3.6
     */
    REQUEST_ERROR_SERIALIZATION(497),

    /**
     * Code 500: A general server error occurred that prevented the request from being processed.
     *
     * @since 3.0.0-incubating
     */
    SERVER_ERROR(500);

    private final int value;
    private final static Map<Integer, ResponseStatusCode> codeValueMap = new HashMap<>();

    static {
        Stream.of(ResponseStatusCode.values()).forEach(code -> codeValueMap.put(code.getValue(), code));
    }

    public static ResponseStatusCode getFromValue(final int codeValue) {
        return codeValueMap.get(codeValue);
    }

    ResponseStatusCode(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public boolean isSuccess() {
        return String.valueOf(this.value).startsWith("2");
    }

    /**
     * Indicates whether the status code can only be used in the last response for a particular request.
     */
    public boolean isFinalResponse() {
        return this != PARTIAL_CONTENT && this != AUTHENTICATE;
    }
}
