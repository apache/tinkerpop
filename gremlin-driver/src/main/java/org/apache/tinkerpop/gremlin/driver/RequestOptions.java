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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Options that can be supplied on a per request basis.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class RequestOptions {

    public static final RequestOptions EMPTY = RequestOptions.build().create();

    private final Map<String,String> aliases;
    private final Map<String, Object> parameters;
    private final Integer batchSize;
    private final Long timeout;

    private RequestOptions(final Builder builder) {
        this.aliases = builder.aliases;
        this.parameters = builder.parameters;
        this.batchSize = builder.batchSize;
        this.timeout = builder.timeout;
    }

    public Optional<Map<String, String>> getAliases() {
        return Optional.ofNullable(aliases);
    }

    public Optional<Map<String, Object>> getParameters() {
        return Optional.ofNullable(parameters);
    }

    public Optional<Integer> getBatchSize() {
        return Optional.ofNullable(batchSize);
    }

    public Optional<Long> getTimeout() {
        return Optional.ofNullable(timeout);
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {
        private Map<String,String> aliases = null;
        private Map<String, Object> parameters = null;
        private Integer batchSize = null;
        private Long timeout = null;

        /**
         * The aliases to set on the request.
         */
        public Builder addAlias(final String aliasName, final String actualName) {
            if (null == aliases)
                aliases = new HashMap<>();

            aliases.put(aliasName, actualName);
            return this;
        }

        /**
         * The parameters to pass on the request.
         */
        public Builder addParameter(final String name, final Object value) {
            if (null == parameters)
                parameters = new HashMap<>();

            parameters.put(name, value);
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
         * The per client request override in milliseconds for the server configured {@code scriptEvaluationTimeout}.
         * If this value is not set, then the configuration for the server is used.
         */
        public Builder timeout(final long timeout) {
            this.timeout = timeout;
            return this;
        }

        public RequestOptions create() {
            return new RequestOptions(this);
        }

    }
}
