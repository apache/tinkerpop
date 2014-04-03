package com.tinkerpop.gremlin.server.message;

import com.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The model for a request message sent to the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessage {
    /**
     * An "invalid" message.  Used internally only.
     */
    public static RequestMessage INVALID = new RequestMessage("invalid");

    /**
     * The id of the current request and is used to track the message within Gremlin Server and in its response.  This
     * value should be unique per request made.
     */
    public UUID requestId = null;

    /**
     * The operation or command to perform as defined by a particular {@link com.tinkerpop.gremlin.server.OpProcessor}.
     */
    public String op;

    /**
     * The name of the {@link com.tinkerpop.gremlin.server.OpProcessor} that should handle the {@link #op}.  Defaults to the
     * {@link StandardOpProcessor} if not specified.
     */
    public String processor = StandardOpProcessor.OP_PROCESSOR_NAME;

    /**
     * A {@link Map} of arguments that are supplied to the {@link #op}.  Each {@link #op} accepts different argument,
     * so consult the documentation for a particular one to understand what is expected.
     */
    public Map<String, Object> args = new HashMap<>();

    private RequestMessage() {
    }

    private RequestMessage(final String op) {
        this.op = op;
    }

    public <T> Optional<T> optionalArgs(final String key) {
        final Object o = args.get(key);
        return o == null ? Optional.empty() : Optional.of((T) o);
    }

    public static Builder create(final String op) {
        return new Builder(op);
    }

    /**
     * Builder class for {@link RequestMessage}.
     */
    public static final class Builder {
        private UUID requestId = UUID.randomUUID();
        private String op;
        private String processor = StandardOpProcessor.OP_PROCESSOR_NAME;
        private Map<String, Object> args = new HashMap<>();

        private Builder(final String op) {
            this.op = op;
        }

        /**
         * If this value is not set in the builder then the {@link RequestMessage#processor} defaults to
         * {@link StandardOpProcessor}.
         *
         * @param processor the name of the processor
         */
        public Builder setProcessor(final String processor) {
            this.processor = processor;
            return this;
        }

        /**
         * Override the request identifier with a specified one, otherwise the {@link Builder} will randomly generate
         * a {@link UUID}.
         */
        public Builder overrideRequestId(final UUID requestId) {
            this.requestId = requestId;
            return this;
        }

        /**
         * Create the request message given the settings provided to the {@link Builder}.
         */
        public RequestMessage build() {
            final RequestMessage msg = new RequestMessage();
            msg.args = this.args;
            msg.op = this.op;
            msg.processor = this.processor;
            msg.requestId = this.requestId;

            return msg;
        }
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                ", requestId=" + requestId +
                ", op='" + op + '\'' +
                ", processor='" + processor + '\'' +
                ", args=" + args +
                '}';
    }
}
