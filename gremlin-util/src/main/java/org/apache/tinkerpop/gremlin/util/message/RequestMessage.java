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

import org.apache.tinkerpop.gremlin.util.Tokens;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The model for a request message in the HTTP body that is sent to the server beginning in 4.0.0.
 */
public final class RequestMessage {
    private String gremlin;
    private Map<String, Object> fields;

    private RequestMessage(final String gremlin, final Map<String, Object> fields) {
        if (null == gremlin) throw new IllegalArgumentException("RequestMessage requires gremlin argument");

        this.gremlin = gremlin;
        this.fields = fields;

        this.fields.putIfAbsent(Tokens.ARGS_LANGUAGE, "gremlin-lang");
    }

    /**
     * Empty constructor for serialization.
     */
    private RequestMessage() { }

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

    public String getGremlin() {
        return gremlin;
    }

    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    public RequestMessage trimMessage(int size) {
        gremlin = gremlin.substring(0, size) + "...";
        return this;
    }

    public static Builder from(final RequestMessage msg) {
        final Builder builder = build(msg.gremlin);
        builder.fields.putAll(msg.getFields());
        if (msg.getFields().containsKey(Tokens.ARGS_BINDINGS)) {
            builder.addBindings((Map<String, Object>) msg.getFields().get(Tokens.ARGS_BINDINGS));
        }
        return builder;
    }

    public static Builder from(final RequestMessage msg, final String gremlin) {
        final Builder builder = build(gremlin);
        builder.fields.putAll(msg.getFields());
        if (msg.getFields().containsKey(Tokens.ARGS_BINDINGS)) {
            builder.addBindings((Map<String, Object>) msg.getFields().get(Tokens.ARGS_BINDINGS));
        }
        return builder;
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                ", fields=" + fields +
                ", gremlin=" + gremlin +
                '}';
    }

    public static Builder build(final String gremlin) {
        return new Builder(gremlin);
    }

    /**
     * Builder class for {@link RequestMessage}.
     */
    public static final class Builder {
        private final String gremlin;
        private final Map<String, Object> bindings = new HashMap<>();
        private final Map<String, Object> fields = new HashMap<>(); // Only allow certain items to be added to prevent breaking changes.

        private Builder(final String gremlin) {
            this.gremlin = gremlin;
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
            if (timeout < 0) throw new IllegalArgumentException("timeout argument cannot be negative.");

            this.fields.put(Tokens.TIMEOUT_MS, timeout);
            return this;
        }

        public Builder addBulkResults(final boolean bulking) {
            this.fields.put(Tokens.BULK_RESULTS, String.valueOf(bulking));
            return this;
        }

        /**
         * Create the request message given the settings provided to the {@link Builder}.
         */
        public RequestMessage create() {
            this.fields.put(Tokens.ARGS_BINDINGS, bindings);
            return new RequestMessage(gremlin, fields);
        }
    }
}
