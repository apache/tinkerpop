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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ResponseMessageV4 {
    private final ResponseStatusV4 responseStatus;
    private final ResponseResult responseResult;

    private ResponseMessageV4(final ResponseStatusV4 responseStatus,
                            final ResponseResult responseResult) {
        this.responseResult = responseResult;
        this.responseStatus = responseStatus;
    }

    public ResponseStatusV4 getStatus() {
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
        private final ResponseMessageV4 responseMessage;
        private final boolean typed;

        public ResponseMessageHeader(final ResponseMessageV4 responseMessage, final boolean typed) {
            this.responseMessage = responseMessage;
            this.typed = typed;
        }

        public ResponseMessageV4 getResponseMessage() {
            return responseMessage;
        }

        public boolean getTyped() {
            return typed;
        }
    }

    public static class ResponseMessageFooter {
        private final ResponseMessageV4 responseMessage;
        private final boolean typed;

        public ResponseMessageFooter(final ResponseMessageV4 responseMessage, final boolean typed) {
            this.responseMessage = responseMessage;
            this.typed = typed;
        }

        public ResponseMessageV4 getResponseMessage() {
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
        private Object result = Collections.emptyList();
        private String statusMessage = null;
        private String exception = null;
        private Map<String, Object> attributes = Collections.emptyMap();
        private Map<String, Object> metaData = Collections.emptyMap();

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

        public ResponseMessageV4 create() {
            final ResponseResult responseResult = new ResponseResult(result, metaData);
            // skip null values
            if (code == null && statusMessage == null) {
                return new ResponseMessageV4(null, responseResult);
            }
            final ResponseStatusV4 responseStatus = new ResponseStatusV4(code, statusMessage, exception);
            return new ResponseMessageV4(responseStatus, responseResult);
        }
    }
}
