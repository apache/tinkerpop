package com.tinkerpop.gremlin.driver.message;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseMessage {

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

    public static Builder build(final RequestMessage requestMessage) {
        return new Builder(requestMessage);
    }

    public static Builder build(final UUID requestId) {
        return new Builder(requestId);
    }

    public static class Builder {

        private final UUID requestId;
        private ResponseStatusCode code = ResponseStatusCode.SUCCESS;
        private Object result = null;
        private String statusMessage = "";
        private Map<String, Object> attributes = Collections.emptyMap();
        private Map<String, Object> metaData = Collections.emptyMap();

        private Builder(final RequestMessage requestMessage) {
            this.requestId = requestMessage.getRequestId();
        }

        private Builder(final UUID requestId) {
            this.requestId = requestId;
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
            final ResponseStatus responseStatus = new ResponseStatus(code, statusMessage, attributes);
            return new ResponseMessage(requestId, responseStatus, responseResult);
        }
    }
}
