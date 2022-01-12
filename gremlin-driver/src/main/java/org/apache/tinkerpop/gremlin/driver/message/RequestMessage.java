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
package org.apache.tinkerpop.gremlin.driver.message;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The model for a request message sent to the server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class RequestMessage {
    /**
     * An "invalid" message.  Used internally only.
     */
    public static final RequestMessage INVALID = new RequestMessage(Tokens.OPS_INVALID);

    private final UUID requestId;
    private final String op;
    private final String processor;
    private final Map<String, Object> args;

    private RequestMessage(final UUID requestId, final String op, final String processor, final Map<String, Object> args) {
        this.requestId = requestId;
        this.op = op;
        this.processor = processor;
        this.args = Optional.ofNullable(args).orElse(new HashMap<>());
    }

    /**
     * Empty constructor for serialization.
     */
    private RequestMessage() {
        this(null);
    }

    private RequestMessage(final String op) {
        this(null, op, null, null);
    }

    /**
     * The id of the current request and is used to track the message within Gremlin Server and in its response.  This
     * value should be unique per request made.
     */
    public UUID getRequestId() {
        return requestId;
    }

    /**
     * The operation or command to perform as defined by a particular Processor.
     */
    public String getOp() {
        return op;
    }

    /**
     * The name of the Processor that should handle the {@link #op}.  Defaults to the standard processor if
     * not specified.
     */
    public String getProcessor() {
        return processor;
    }

    /**
     * A {@link Map} of arguments that are supplied to the {@link #op}.  Each {@link #op} accepts different argument,
     * so consult the documentation for a particular one to understand what is expected.
     */
    public Map<String, Object> getArgs() {
        return args;
    }

    public <T> Optional<T> optionalArgs(final String key) {
        final Object o = args.get(key);
        return o == null ? Optional.empty() : Optional.of((T) o);
    }

    public static Builder from(final RequestMessage msg) {
        final Builder builder = build(msg.op)
                .overrideRequestId(msg.requestId)
                .processor(msg.processor);
        msg.args.forEach(builder::addArg);
        return builder;
    }

    public static Builder build(final String op) {
        return new Builder(op);
    }

    /**
     * Builder class for {@link RequestMessage}.
     */
    public static final class Builder {
        public static final String OP_PROCESSOR_NAME = "";
        private UUID requestId;
        private String op;
        private String processor = OP_PROCESSOR_NAME;
        private Map<String, Object> args = new HashMap<>();

        private Builder(final String op) {
            this.op = op;
        }

        /**
         * If this value is not set in the builder then the {@link RequestMessage#processor} defaults to
         * the standard op processor (empty string).
         *
         * @param processor the name of the processor
         */
        public Builder processor(final String processor) {
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

        public Builder addArg(final String key, final Object val) {
            args.put(key, val);
            return this;
        }

        public Builder add(final Object... keyValues) {
            args.putAll(ElementHelper.asMap(keyValues));
            return this;
        }

        /**
         * Create the request message given the settings provided to the {@link Builder}.
         */
        public RequestMessage create() {
            return new RequestMessage(null == requestId ? UUID.randomUUID() : requestId, op, processor, args);
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
