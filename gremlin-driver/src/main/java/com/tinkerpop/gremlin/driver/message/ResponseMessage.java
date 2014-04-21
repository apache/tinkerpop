package com.tinkerpop.gremlin.driver.message;

import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseMessage {

    /**
     * The current request that generated this response.
     */
    private final UUID requestId;
    private final String contentType;
    private final ResultCode code;
    private final Object result;
    private final ResultType resultType;

    private ResponseMessage(final UUID requestId, final String contentType, final ResultCode code,
                            final Object result, final ResultType resultType) {
        this.requestId = requestId;
        this.contentType = contentType;
        this.code = code;
        this.result = result;
        this.resultType = resultType;
    }

    public ResultType getResultType() {
        return resultType;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public String getContentType() {
        return contentType;
    }

    public ResultCode getCode() {
        return code;
    }

    public Object getResult() {
        return result;
    }

    public static Builder create(final RequestMessage requestMessage) {
        return new Builder(requestMessage);
    }

    public static Builder create(final UUID requestId, final String contentType) {
        return new Builder(requestId, contentType);
    }

    public static class Builder {

        private final UUID requestId;
        private final String contentType;
        private ResultCode code = ResultCode.SUCCESS;
        private Object result = null;
        private ResultType contents = ResultType.OBJECT;

        private Builder(final RequestMessage requestMessage) {
            this.contentType = requestMessage.<String>optionalArgs("accept").orElse("text/plain");
            this.requestId = requestMessage.getRequestId();
        }

        private Builder(final UUID requestId, final String contentType) {
            this.contentType = contentType;
            this.requestId = requestId;
        }

        public Builder code(final ResultCode code) {
            this.code = code;
            return this;
        }

        public Builder result(final Object result) {
            this.result = result;
            return this;
        }

        public Builder contents(final ResultType contents) {
            this.contents = contents;
            return this;
        }

        public ResponseMessage build() {
            return new ResponseMessage(requestId, contentType, code, result, contents);
        }
    }
}
