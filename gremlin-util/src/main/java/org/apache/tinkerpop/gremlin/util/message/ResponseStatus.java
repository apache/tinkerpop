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

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ResponseStatus {
    private final HttpResponseStatus code;
    private final String message;
    private final String exception;

    public ResponseStatus(final HttpResponseStatus code, final String message, final String exception) {
        this.code = code;
        this.message = message;
        this.exception = exception;
    }

    /**
     * Gets the {@link HttpResponseStatus} that describes how the server responded to the request.
     */
    public HttpResponseStatus getCode() {
        return code;
    }

    /**
     * Gets the message associated with the code.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the exception in case of error.
     */
    public String getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "ResponseStatus{" +
                "code=" + code +
                ", message='" + message + '\'' +
                ", exception=" + exception +
                '}';
    }
}
