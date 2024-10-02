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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This strategy will not alter the traversal. It is only a holder for configuration options associated with the
 * traversal meant to be accessed by steps or other classes that might have some interaction with it. It is
 * essentially a way for users to provide traversal level configuration options that can be used in various ways by
 * different graph providers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OptionsStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    /**
     * An empty {@code OptionsStrategy} with no configuration values inside.
     */
    public static final OptionsStrategy EMPTY = OptionsStrategy.build().create();

    private final Map<String, Object> options;

    private OptionsStrategy(final Builder builder) {
        options = builder.options;
    }

    /**
     * Gets the options on the strategy as an immutable {@code Map}.
     */
    public Map<String,Object> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    public Configuration getConfiguration() {
        return new MapConfiguration(options);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // has not effect on the traversal itself - simply carries a options with it that individual steps
        // can choose to use or not.
    }

    public static OptionsStrategy create(final Configuration configuration) {
        final Builder builder = build();
        configuration.getKeys().forEachRemaining(k -> builder.with(k, configuration.getProperty(k)));
        return builder.create();
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, Object> options = new HashMap<>();

        /**
         * Adds a key to the configuration with the value of {@code true}.
         */
        public Builder with(final String key) {
            return with(key, true);
        }

        /**
         * Adds an option to the configuration.
         */
        public Builder with(final String key, final Object value) {
            options.put(key, value);
            return this;
        }

        public OptionsStrategy create() {
            return new OptionsStrategy(this);
        }
    }
}
