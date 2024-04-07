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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ResponseMessage {

    /**
     * The current request that generated this response.
     */
    private final UUID requestId;
    private final ResponseStatus responseStatus;
    private final ResponseResult responseResult;

    private ResponseMessage(final UUID requestId, final ResponseStatus responseStatus,
                            final ResponseResult responseResult) {
        this.requestId = requestId;
        this.responseResult = responseResult;
        this.responseStatus = responseStatus;
    }

    public UUID getRequestId() {
        return requestId;
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
                "requestId=" + requestId +
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

    public static Builder build(final RequestMessage requestMessage) {
        return new Builder(requestMessage);
    }

    public static Builder build(final RequestMessageV4 requestMessage) {
        return new Builder(requestMessage);
    }

    public static Builder build(final UUID requestId) {
        return new Builder(requestId);
    }

    public static Builder buildV4(final UUID requestId) {
        return new Builder(requestId, true);
    }

    public final static class Builder {

        private final UUID requestId;
        private ResponseStatusCode code = ResponseStatusCode.SUCCESS;
        private Object result = null;
        private String statusMessage = "";
        private Map<String, Object> attributes = Collections.emptyMap();
        private Map<String, Object> metaData = Collections.emptyMap();

        private Builder(final RequestMessage requestMessage) {
            this.requestId = requestMessage.getRequestId();
        }

        private Builder(final RequestMessageV4 requestMessage) {
            this.requestId = requestMessage.getRequestId();
        }

        private Builder(final UUID requestId) {
            this.requestId = requestId;
        }

        // builder for TP4
        private Builder(final UUID requestId, final boolean v4) {
            this.requestId = requestId;
            this.code = null;
            this.statusMessage = null;
        }

        public Builder code(final ResponseStatusCode code) {
            this.code = code;
            return this;
        }

        public Builder statusMessage(final String message) {
            this.statusMessage = message;
            return this;
        }

        public Builder statusAttributes(final Map<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder statusAttributeException(final Throwable ex) {
            statusAttribute(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS, IteratorUtils.asList(
                    IteratorUtils.map(ExceptionUtils.getThrowableList(ex), t -> t.getClass().getName())));
            statusAttribute(Tokens.STATUS_ATTRIBUTE_STACK_TRACE, ExceptionUtils.getStackTrace(ex));
            return this;
        }

        public Builder statusAttribute(final String key, final Object value) {
            if (this.attributes == Collections.EMPTY_MAP)
                attributes = new HashMap<>();
            attributes.put(key, value);
            return this;
        }

        public Builder result(final Object result) {
            this.result = result;
            return this;
        }

        public Builder responseMetaData(final Map<String, Object> metaData) {
            this.metaData = metaData;
            return this;
        }

        public ResponseMessage create() {
            final ResponseResult responseResult = new ResponseResult(result, metaData);
            // skip null values
            if (code == null && statusMessage == null) {
                return new ResponseMessage(requestId, null, responseResult);
            }
            final ResponseStatus responseStatus = new ResponseStatus(code, statusMessage, attributes);
            return new ResponseMessage(requestId, responseStatus, responseResult);
        }
    }
}
