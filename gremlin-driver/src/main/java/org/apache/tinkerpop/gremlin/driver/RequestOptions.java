/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_BATCH_SIZE;
import static org.apache.tinkerpop.gremlin.util.Tokens.BULK_RESULTS;
import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_G;
import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_LANGUAGE;
import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_MATERIALIZE_PROPERTIES;

/**
 * Options that can be supplied on a per request basis.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class RequestOptions {

    public static final RequestOptions EMPTY = RequestOptions.build().create();

    private final String graphOrTraversalSource;
    private final String parameters;
    private final Integer batchSize;
    private final Long timeout;
    private final String language;
    private final String materializeProperties;
    private final String bulkResults;
    private final String transactionId;

    private RequestOptions(final Builder builder) {
        this.graphOrTraversalSource = builder.graphOrTraversalSource;
        this.parameters = builder.parametersString;
        this.batchSize = builder.batchSize;
        this.timeout = builder.timeout;
        this.language = builder.language;
        this.materializeProperties = builder.materializeProperties;
        this.bulkResults = builder.bulkResults;
        this.transactionId = builder.transactionId;
    }

    public Optional<String> getG() {
        return Optional.ofNullable(graphOrTraversalSource);
    }

    public Optional<String> getParameters() {
        return Optional.ofNullable(parameters);
    }

    public Optional<Integer> getBatchSize() {
        return Optional.ofNullable(batchSize);
    }

    public Optional<Long> getTimeout() {
        return Optional.ofNullable(timeout);
    }

    public Optional<String> getLanguage() {
        return Optional.ofNullable(language);
    }

    public Optional<String> getMaterializeProperties() { return Optional.ofNullable(materializeProperties); }

    public Optional<String> getBulkResults() { return Optional.ofNullable(bulkResults); }

    public Optional<String> getTransactionId() { return Optional.ofNullable(transactionId); }

    public static Builder build() {
        return new Builder();
    }

    public static RequestOptions getRequestOptions(final GremlinLang gremlinLang) {
        final Iterator<OptionsStrategy> itty = gremlinLang.getOptionsStrategies().iterator();
        final RequestOptions.Builder builder = RequestOptions.build();
        while (itty.hasNext()) {
            final OptionsStrategy optionsStrategy = itty.next();
            final Map<String, Object> options = optionsStrategy.getOptions();
            if (options.containsKey(ARGS_EVAL_TIMEOUT))
                builder.timeout(((Number) options.get(ARGS_EVAL_TIMEOUT)).longValue());
            if (options.containsKey(ARGS_BATCH_SIZE))
                builder.batchSize(((Number) options.get(ARGS_BATCH_SIZE)).intValue());
            if (options.containsKey(ARGS_MATERIALIZE_PROPERTIES))
                builder.materializeProperties((String) options.get(ARGS_MATERIALIZE_PROPERTIES));
            if (options.containsKey(ARGS_LANGUAGE))
                builder.language((String) options.get(ARGS_LANGUAGE));
            if (options.containsKey(BULK_RESULTS))
                builder.bulkResults((boolean) options.get(BULK_RESULTS));
        }
        // request the server to bulk results by default when using DRC through request options
        if (builder.bulkResults == null)
            builder.bulkResults(true);

        final String parametersString = gremlinLang.getParametersAsString();
        if (!"[:]".equals(parametersString)) {
            builder.addParametersString(parametersString);
        }
        return builder.create();
    }

    public static final class Builder {
        private String graphOrTraversalSource = null;
        private Map<String, Object> internalParameters = null;
        private String parametersString = null;
        private Integer batchSize = null;
        private Long timeout = null;
        private String materializeProperties = null;
        private String language = null;
        private String bulkResults = null;
        private String transactionId = null;

        /**
         * Creates a {@link Builder} populated with the values from the provided {@link RequestOptions}.
         * @param options the options to copy from
         * @return a {@link Builder} with the copied options
         */
        public static Builder from(final RequestOptions options) {
            final Builder builder = build();
            builder.graphOrTraversalSource = options.graphOrTraversalSource;
            builder.parametersString = options.parameters;
            builder.batchSize = options.batchSize;
            builder.timeout = options.timeout;
            builder.materializeProperties = options.materializeProperties;
            builder.language = options.language;
            builder.bulkResults = options.bulkResults;
            builder.transactionId = options.transactionId;
            return builder;
        }

        /**
         * Sets the traversal source (or graph) alias to rebind to "g" on the request.
         */
        public Builder traversalSource(final String traversalSource) {
            this.graphOrTraversalSource = traversalSource;
            return this;
        }

        /**
         * The aliases to set on the request.
         *
         * @deprecated As of release 4.0.0, replaced by {@link #traversalSource(String)}.
         */
        @Deprecated
        public Builder addG(final String graphOrTraversalSource) {
            this.graphOrTraversalSource = graphOrTraversalSource;
            return this;
        }

        /**
         * Adds a parameter to the request. The parameter map is converted to a gremlin-lang map literal string when the
         * request is built. This method accumulates parameters into an internal map which is converted on
         * {@link #create()}.
         * <p>
         * Cannot be mixed with {@link #addParametersString(String)}.
         */
        public Builder addParameter(final String name, final Object value) {
            if (parametersString != null)
                throw new IllegalStateException("Cannot mix addParameter() with addParametersString()");

            if (internalParameters == null) internalParameters = new HashMap<>();

            if (ARGS_G.equals(name)) {
                this.graphOrTraversalSource = (String) value;
            }

            if (ARGS_LANGUAGE.equals(name)) {
                this.language = (String) value;
            }

            internalParameters.put(name, value);
            return this;
        }

        /**
         * Sets the parameters as a pre-formatted gremlin-lang map literal string. This allows users to pass parameters
         * as a raw gremlin-lang string when using the Client API with a gremlin string.
         * <p>
         * Cannot be mixed with {@link #addParameter(String, Object)}.
         */
        public Builder addParametersString(final String parametersString) {
            if (internalParameters != null)
                throw new IllegalStateException("Cannot mix addParametersString() with addParameter()");

            this.parametersString = parametersString;
            return this;
        }

        /**
         * The per client request override for the client and server configured {@code resultIterationBatchSize}. If
         * this value is not set, then the configuration for the {@link Cluster} is used unless the
         * {@link RequestMessage} is configured completely by the user.
         */
        public Builder batchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * The per client request override in milliseconds for the server configured {@code evaluationTimeout}.
         * If this value is not set, then the configuration for the server is used.
         */
        public Builder timeout(final long timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets the language identifier to be sent on the request.
         */
        public Builder language(final String language) {
            this.language = language;
            return this;
        }

        /**
         * Sets the materializeProperties identifier to be sent on the request.
         */
        public Builder materializeProperties(final String materializeProperties) {
            this.materializeProperties = materializeProperties;
            return this;
        }

        /**
         * Sets the bulkResults flag to be sent on the request. A value of turn will enable server to bulk results.
         */
        public Builder bulkResults(final boolean bulking) {
            this.bulkResults = String.valueOf(bulking);
            return this;
        }

        /**
         * Sets the transactionId value to be sent on the request.
         */
        public Builder transactionId(final String id) {
            this.transactionId = id;
            return this;
        }

        public RequestOptions create() {
            // if parameters were added via addParameter(), convert the map to a string
            if (internalParameters != null) {
                parametersString = GremlinLang.convertParametersToString(internalParameters);
            }
            return new RequestOptions(this);
        }
    }
}
