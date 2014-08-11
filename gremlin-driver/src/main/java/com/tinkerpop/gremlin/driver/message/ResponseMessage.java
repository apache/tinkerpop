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
    private final ResultCode code;
    private final Object result;
    private final ResultType resultType;

    private ResponseMessage(final UUID requestId, final ResultCode code,
                            final Object result, final ResultType resultType) {
        this.requestId = requestId;
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

    public ResultCode getCode() {
        return code;
    }

    public Object getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "ResponseMessage{" +
                "requestId=" + requestId +
                ", code=" + code +
                ", result=" + result +
                ", resultType=" + resultType +
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
        private ResultCode code = ResultCode.SUCCESS;
        private Object result = null;
        private ResultType contents = ResultType.OBJECT;

        private Builder(final RequestMessage requestMessage) {
            this.requestId = requestMessage.getRequestId();
        }

        private Builder(final UUID requestId) {
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

        public ResponseMessage create() {
            return new ResponseMessage(requestId, code, result, contents);
        }
    }
}
