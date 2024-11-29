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
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A strategy that resets the specified {@code seed} value for {@link Seedable} steps, which in turn will produce
 * deterministic results from those steps. It is important to note that when using this strategy that it only
 * guarantees deterministic results from a step but not from an entire traversal. For example, if a graph does no
 * guarantee iteration order for {@code g.V()} then repeated runs of {@code g.V().coin(0.5)} with this strategy
 * will return the same number of results but not necessarily the same ones. The same problem can occur in OLAP-based
 * traversals where iteration order is not explicitly guaranteed. The only way to ensure completely deterministic
 * results in that sense is to apply some form of {@code order()} in these cases
 */
public class SeedStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private final long seed;

    /**
     * @deprecated As of release 3.7.3, replaced by {@link #build()#seed}.
     */
    @Deprecated
    public SeedStrategy(final long seed) {
        this.seed = seed;
    }

    public long getSeed() {
        return seed;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final List<Seedable> seedableSteps = TraversalHelper.getStepsOfAssignableClass(Seedable.class, traversal);
        for (final Seedable seedableStepsToReset : seedableSteps) {
            seedableStepsToReset.resetSeed(seed);
        }
    }

    public static final String ID_SEED = "seed";

    public static SeedStrategy create(final Configuration configuration) {
        if (!configuration.containsKey(ID_SEED))
            throw new IllegalArgumentException("SeedStrategy configuration requires a 'seed' value");

        return new SeedStrategy(Long.parseLong(configuration.getProperty(ID_SEED).toString()));
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(ID_SEED, this.seed);
        map.put(STRATEGY, SeedStrategy.class.getCanonicalName());
        return new MapConfiguration(map);
    }

    /**
     * Builds a {@code SeedStrategy} instance.
     */
    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private long seed = 0;

        /**
         * Set the seed value for the strategy that will ensure deterministic results from {@link Seedable} steps.
         */
        public Builder seed(final long seed) {
            this.seed = seed;
            return this;
        }

        public SeedStrategy create() {
            return new SeedStrategy(seed);
        }
    }
}
