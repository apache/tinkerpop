package com.tinkerpop.gremlin.server.message;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseMessage {

    /**
     * The current request that generated this response.
     */
    private final RequestMessage requestMessage;
    private final ResultCode code;
    private final Object result;

    private ResponseMessage(final RequestMessage requestMessage, final ResultCode code, final Object result) {
        this.requestMessage = requestMessage;
        this.code = code;
        this.result = result;
    }

    public RequestMessage getRequestMessage() {
        return requestMessage;
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

    public static class Builder {
        private final RequestMessage requestMessage;

        private ResultCode code;
        private Object result;

        public Builder(final RequestMessage requestMessage) {
            this.requestMessage = requestMessage;
        }

        public Builder code(final ResultCode code) {
            this.code = code;
            return this;
        }

        public Builder result(final Object result) {
            this.result = result;
            return this;
        }

        public ResponseMessage build() {
            return new ResponseMessage(requestMessage, code, result);
        }
    }
}
