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
package org.apache.tinkerpop.gremlin.driver.message;

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
     * The server successfully processed a request to completion - there are no messages remaining in this stream.
     */
    SUCCESS(200),

    /**
     * The server processed the request but there is no result to return (e.g. an {@link Iterator} with no elements).
     */
    NO_CONTENT(204),

    /**
     * The server successfully returned some content, but there is more in the stream to arrive - wait for a
     * {@link #SUCCESS} to signify the end of the stream.
     */
    PARTIAL_CONTENT(206),

    /**
     * The server could not authenticate the request or the client requested a resource it did not have access to.
     */
    UNAUTHORIZED(401),

    /**
     * The server could authenticate the request, but will not fulfill it.  This is a general purpose code that
     * would typically be returned if the request is authenticated but not authorized to do what it is doing. This
     * code is for future use.
     */
    FORBIDDEN(403),

    /**
     * A challenge from the server for the client to authenticate its request.
     */
    AUTHENTICATE(407),

    /**
     * The request message was not properly formatted which means it could not be parsed at all or the "op" code was
     * not recognized such that Gremlin Server could properly route it for processing.  Check the message format and
     * retry the request.
     */
    REQUEST_ERROR_MALFORMED_REQUEST(498),

    /**
     * The request message was parseable, but the arguments supplied in the message were in conflict or incomplete.
     * Check the message format and retry the request.
     */
    REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS(499),

    /**
     * A general server error occurred that prevented the request from being processed.
     */
    SERVER_ERROR(500),

    /**
     * The script submitted for processing evaluated in the {@code ScriptEngine} with errors and could not be
     * processed.  Check the script submitted for syntax errors or other problems and then resubmit.
     */
    SERVER_ERROR_SCRIPT_EVALUATION(597),

    /**
     * The server exceeded one of the timeout settings for the request and could therefore only partially responded
     * or did not respond at all.
     */
    SERVER_ERROR_TIMEOUT(598),

    /**
     * The server was not capable of serializing an object that was returned from the script supplied on the request.
     * Either transform the object into something Gremlin Server can process within the script or install mapper
     * serialization classes to Gremlin Server.
     */
    SERVER_ERROR_SERIALIZATION(599);

    private final int value;
    private static Map<Integer, ResponseStatusCode> codeValueMap = new HashMap<>();

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
