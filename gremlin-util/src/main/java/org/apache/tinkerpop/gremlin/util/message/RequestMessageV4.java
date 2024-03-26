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

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.util.Tokens;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The model for a request message sent to the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class RequestMessageV4 {
    /**
     * An "invalid" message.  Used internally only.
     */
    public static final RequestMessageV4 INVALID = new RequestMessageV4();
    private UUID requestId;
//
    private String gremlinType; // Type information needed to help deserialize "gremlin" into either String/Bytecode.
//
    private Object gremlin; // Should be either a String or Bytecode type.

    private Map<String, Object> fields;
//
//    private String language;
//
//    private Map<String, Object> bindings;
//
//    private String g;

    private RequestMessageV4(final Object gremlin, final Map<String, Object> fields) {
        if (null == gremlin) throw new IllegalArgumentException("RequestMessage requires gremlin argument");
        if (!(gremlin instanceof Bytecode || gremlin instanceof String)) {
            throw new IllegalArgumentException("gremlin argument for RequestMessage must be either String or Bytecode");
        }

        Object requestId = fields.get(Tokens.REQUEST_ID);
        if (null == requestId) throw new IllegalArgumentException("RequestMessage requires a requestId");
        if (!(requestId instanceof UUID)) {
            throw new IllegalArgumentException("requestId argument for RequestMessage must be a UUID");
        }

        this.gremlin = gremlin;
        this.fields = fields;
        this.requestId = (UUID) requestId;

        if (gremlin instanceof String) {
            gremlinType = Tokens.OPS_EVAL;
            this.fields.putIfAbsent(Tokens.ARGS_LANGUAGE, "gremlin-groovy");
        } else if (gremlin instanceof Bytecode) {
            gremlinType = Tokens.OPS_BYTECODE;
        } else {
            gremlinType = Tokens.OPS_INVALID;
        }

        this.fields.put("gremlinType", gremlinType);
    }

    /**
     * Empty constructor for serialization.
     */
    private RequestMessageV4() { }

    /**
     * The id of the current request and is used to track the message within Gremlin Server and in its response.  This
     * value should be unique per request made.
     */
    public UUID getRequestId() {
        return requestId;
    }

    public <T> Optional<T> optionalArgs(final String key) {
        final Object o = fields.get(key);
        return o == null ? Optional.empty() : Optional.of((T) o);
    }

    public <T> T getArg(final String key) {
        return (T) fields.get(key);
    }

    public <T> T getArgOrDefault(final String key, final T def) {
        return (T) optionalArgs(key).orElse(def);
    }

    public String getGremlinType() {
        return gremlinType;
    }

    public Object getGremlin() {
        return gremlin;
    }

    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    public static Builder from(final RequestMessageV4 msg) {
        final Builder builder = build(msg.gremlin)
                .overrideRequestId(msg.requestId)
                .addLanguage(msg.getArg(Tokens.ARGS_LANGUAGE))
                .addG(msg.getArg(Tokens.ARGS_G))
                .addBindings(msg.getArg(Tokens.ARGS_BINDINGS));
        return builder;
    }

    public static Builder from(final RequestMessageV4 msg, final Object gremlin) {
        final Builder builder = build(gremlin)
                .overrideRequestId(msg.requestId)
                .addLanguage(msg.getArg(Tokens.ARGS_LANGUAGE))
                .addG(msg.getArg(Tokens.ARGS_G))
                .addBindings(msg.getArg(Tokens.ARGS_BINDINGS));
        return builder;
    }

    @Override
    public String toString() {
        return "RequestMessageV4{" +
                ", fields=" + fields +
                ", gremlin=" + gremlin +
                '}';
    }

    public RequestMessage convertToV1() {
        Map<String, String> alias = new HashMap<>();
        if (fields.containsKey(Tokens.ARGS_G)) alias.put(Tokens.ARGS_G, (String) fields.get(Tokens.ARGS_G));

        RequestMessage.Builder builder = RequestMessage.build(this.gremlinType);
        builder.overrideRequestId(this.requestId).addArg(Tokens.ARGS_GREMLIN, this.gremlin).addArg(Tokens.ARGS_ALIASES, alias);
        if (fields.containsKey(Tokens.ARGS_LANGUAGE)) builder.addArg(Tokens.ARGS_LANGUAGE, fields.get(Tokens.ARGS_LANGUAGE));
        if (fields.containsKey(Tokens.ARGS_BINDINGS)) builder.addArg(Tokens.ARGS_BINDINGS, fields.get(Tokens.ARGS_BINDINGS));

        return builder.create();
    }

    public static Builder build(final Object gremlin) {
        return new Builder(gremlin);
    }

    /**
     * Builder class for {@link RequestMessageV4}.
     */
    public static final class Builder {
        private final Object gremlin; // Should be either a String or Bytecode type.

        private Map<String, Object> bindings = new HashMap<>();

        private Map<String, Object> fields = new HashMap<>();

        private Builder(final Object gremlin) {
            this.gremlin = gremlin;
            this.fields.put(Tokens.REQUEST_ID, UUID.randomUUID());
        }

        /**
         * Override the request identifier with a specified one, otherwise the {@link Builder} will randomly generate
         * a {@link UUID}.
         */
        public Builder overrideRequestId(final UUID requestId) {
            this.fields.put(Tokens.REQUEST_ID, requestId);
            return this;
        }

        public Builder addLanguage(final String language) {
            this.fields.put(Tokens.ARGS_LANGUAGE, language);
            return this;
        }

        public Builder addBinding(final String key, final Object val) {
            bindings.put(key, val);
            return this;
        }

        public Builder addBindings(final Map<String, Object> otherBindings) {
            this.bindings.putAll(otherBindings);
            return this;
        }

        public Builder addG(final String g) {
            this.fields.put(Tokens.ARGS_G, g);
            return this;
        }

        /**
         * Create the request message given the settings provided to the {@link Builder}.
         */
        public RequestMessageV4 create() {
            this.fields.put(Tokens.ARGS_BINDINGS, bindings);
            return new RequestMessageV4(gremlin, fields);
        }
    }
}
