package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessage {
    public UUID sessionId = null;
    public UUID requestId = null;
    public String op;
    public Map<String, Object> args = new HashMap<>();

    public Optional<UUID> optionalSessionId() {
        return sessionId == null ? Optional.empty() : Optional.of(this.sessionId);
    }

    public <T> Optional<T> optionalArgs(final String key) {
        final Object o = args.get(key);
        return  o == null ? Optional.empty() : Optional.of((T) o);
    }

    public static class Serializer {

        private static final ObjectMapper mapper = new ObjectMapper();

        public static RequestMessage parse(final String input) {
            RequestMessage requestMessage = null;
            try {
                requestMessage = mapper.readValue(input, RequestMessage.class);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            return requestMessage;
        }
    }
}
