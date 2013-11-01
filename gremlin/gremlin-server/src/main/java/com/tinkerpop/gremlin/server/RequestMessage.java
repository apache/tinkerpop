package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessage {
    public static RequestMessage INVALID = new RequestMessage("invalid");

    public UUID sessionId = null;
    public UUID requestId = null;
    public String op;
    public String processor = StandardOpProcessor.OP_PROCESSOR_NAME;
    public Map<String, Object> args = new HashMap<>();

    public RequestMessage() {}

    public RequestMessage(final String op) {
        this.op = op;
    }

    public Optional<UUID> optionalSessionId() {
        return sessionId == null ? Optional.empty() : Optional.of(this.sessionId);
    }

    public <T> Optional<T> optionalArgs(final String key) {
        final Object o = args.get(key);
        return  o == null ? Optional.empty() : Optional.of((T) o);
    }

    public static class Serializer {

        private static final ObjectMapper mapper = new ObjectMapper();

        public static Optional<RequestMessage> parse(final String input) {
            try {
                return Optional.of(mapper.readValue(input, RequestMessage.class));
            } catch (Exception ex) {
                return Optional.empty();
            }
        }
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                "sessionId=" + sessionId +
                ", requestId=" + requestId +
                ", op='" + op + '\'' +
                ", processor='" + processor + '\'' +
                ", args=" + args +
                '}';
    }
}
