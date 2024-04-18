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
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The model for a request message in the HTTP body that is sent to the server beginning in 4.0.0.
 */
public final class RequestMessageV4 {
    /**
     * An "invalid" message.  Used internally only.
     */
    public static final RequestMessageV4 INVALID = new RequestMessageV4();

    private String gremlinType; // Type information needed to help deserialize "gremlin" into either String/Bytecode.

    private Object gremlin; // Should be either a String or Bytecode type.

    private Map<String, Object> fields;

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
        return getField(Tokens.REQUEST_ID);
    }

    public <T> Optional<T> optionalField(final String key) {
        final Object o = fields.get(key);
        return o == null ? Optional.empty() : Optional.of((T) o);
    }

    public <T> T getField(final String key) {
        return (T) fields.get(key);
    }

    public <T> T getFieldOrDefault(final String key, final T def) {
        return (T) optionalField(key).orElse(def);
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

    public RequestMessageV4 trimMessage(int size) {
        gremlin = gremlin.toString().substring(0, size) + "...";
        return this;
    }

    public static Builder from(final RequestMessageV4 msg) {
        final Builder builder = build(msg.gremlin);
        builder.fields.putAll(msg.getFields());
        return builder;
    }

    public static Builder from(final RequestMessageV4 msg, final Object gremlin) {
        final Builder builder = build(gremlin);
        builder.fields.putAll(msg.getFields());
        return builder;
    }

    @Override
    public String toString() {
        return "RequestMessageV4{" +
                ", fields=" + fields +
                ", gremlin=" + gremlin +
                '}';
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

        public Builder addLanguage(final String language) {
            Objects.requireNonNull(language, "language argument cannot be null.");
            this.fields.put(Tokens.ARGS_LANGUAGE, language);
            return this;
        }

        public Builder addBinding(final String key, final Object val) {
            bindings.put(key, val);
            return this;
        }

        public Builder addBindings(final Map<String, Object> otherBindings) {
            Objects.requireNonNull(otherBindings, "bindings argument cannot be null.");
            this.bindings.putAll(otherBindings);
            return this;
        }

        public Builder addG(final String g) {
            Objects.requireNonNull(g, "g argument cannot be null.");
            this.fields.put(Tokens.ARGS_G, g);
            return this;
        }

        public Builder addChunkSize(final int chunkSize) {
            Objects.requireNonNull(chunkSize, "chunkSize argument cannot be null.");
            this.fields.put(Tokens.ARGS_BATCH_SIZE, chunkSize);
            return this;
        }

        public Builder addMaterializeProperties(final String materializeProps) {
            Objects.requireNonNull(materializeProps, "materializeProps argument cannot be null.");
            if (!materializeProps.equals(Tokens.MATERIALIZE_PROPERTIES_TOKENS) && !materializeProps.equals(Tokens.MATERIALIZE_PROPERTIES_ALL)) {
                throw new IllegalArgumentException("materializeProperties argument must be either token or all.");
            }

            this.fields.put(Tokens.ARGS_MATERIALIZE_PROPERTIES, materializeProps);
            return this;
        }

        public Builder addTimeoutMillis(final long timeout) {
            Objects.requireNonNull(timeout, "timeout argument cannot be null.");
            if (timeout < 0) throw new IllegalArgumentException("timeout argument cannot be negative.");

            this.fields.put(Tokens.ARGS_EVAL_TIMEOUT, timeout);
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
