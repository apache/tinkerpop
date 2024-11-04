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

import java.util.Collections;
import java.util.List;

/**
 * The model for a response message that is sent to the server beginning in 4.0.0. ResponseMessage is designed to be
 * streamed back the client in parts so depending on the state of the transfer, certain parts may be null at different
 * times.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ResponseMessage {
    private final ResponseStatus responseStatus;
    private final ResponseResult responseResult;

    private ResponseMessage(final ResponseStatus responseStatus,
                            final ResponseResult responseResult) {
        this.responseResult = responseResult;
        this.responseStatus = responseStatus;
    }

    public ResponseStatus getStatus() {
        return responseStatus;
    }

    public ResponseResult getResult() {
        return responseResult;
    }

    @Override
    public String toString() {
        return "ResponseMessage{" +
                ", status=" + responseStatus +
                ", result=" + responseResult +
                '}';
    }

    public static class ResponseMessageHeader {
        private final ResponseMessage responseMessage;
        private final boolean typed;

        public ResponseMessageHeader(final ResponseMessage responseMessage, final boolean typed) {
            this.responseMessage = responseMessage;
            this.typed = typed;
        }

        public ResponseMessage getResponseMessage() {
            return responseMessage;
        }

        public boolean getTyped() {
            return typed;
        }
    }

    public static class ResponseMessageFooter {
        private final ResponseMessage responseMessage;
        private final boolean typed;

        public ResponseMessageFooter(final ResponseMessage responseMessage, final boolean typed) {
            this.responseMessage = responseMessage;
            this.typed = typed;
        }

        public ResponseMessage getResponseMessage() {
            return responseMessage;
        }

        public boolean getTyped() {
            return typed;
        }
    }

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder {
        private HttpResponseStatus code = null;
        private List<Object> result = Collections.emptyList();
        private boolean bulked = false;
        private String statusMessage = null;
        private String exception = null;

        private Builder() { }

        public Builder code(final HttpResponseStatus code) {
            this.code = code;
            return this;
        }

        public Builder statusMessage(final String message) {
            this.statusMessage = message;
            return this;
        }

        public Builder exception(final String exception) {
            this.exception = exception;
            return this;
        }

        public Builder result(final List<Object> result) {
            this.result = result;
            return this;
        }

        public Builder bulked(final boolean bulked) {
            this.bulked = bulked;
            return this;
        }

        public ResponseMessage create() {
            final ResponseResult responseResult = new ResponseResult(result, bulked);
            // skip null values
            if (code == null && statusMessage == null) {
                return new ResponseMessage(null, responseResult);
            }
            final ResponseStatus responseStatus = new ResponseStatus(code, statusMessage, exception);
            return new ResponseMessage(responseStatus, responseResult);
        }
    }
}
