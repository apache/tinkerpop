package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RequestMessage {
    public UUID sessionId;
    public UUID requestId;
    public String op;
    public Map<String, Object> args;


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
