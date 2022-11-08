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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A strategy that manages the properties that will be in the result.
 *
 * @author Valentyn Kahamlyk
 */
public final class DetachStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final DetachStrategy INSTANCE = new DetachStrategy(DetachMode.ALL, null);
    private DetachMode detachMode = DetachMode.ALL;
    private Set<String> keys;

    private DetachStrategy() {}

    private DetachStrategy(final DetachMode detachMode, final Set<String> keys) {
        this.detachMode = detachMode;
        this.keys = keys;
    }

    private DetachStrategy(final Builder builder) {
        this(builder.detachMode, builder.keys);
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() == EmptyStep.instance()) {
            final Optional<ProfileSideEffectStep> profileStep = TraversalHelper.getFirstStepOfAssignableClass(ProfileSideEffectStep.class, traversal);
            final int index = profileStep.map(step -> traversal.getSteps().indexOf(step))
                    .orElseGet(() -> traversal.getSteps().size());
            traversal.addStep(index,
                    new DetachElementStep<>(traversal, new DetachOptions(detachMode, keys)));
        }
    }

    public static DetachStrategy create(final Configuration configuration) {
        return new DetachStrategy(DetachMode.valueOf(configuration.getString(ID_MODE)),
                new HashSet<>((Collection<String>) configuration.getProperty(ID_KEYS)));
    }

    /**
     * Gets the standard configuration of this strategy that will return all properties.
     */
    public static DetachStrategy instance() {
        return INSTANCE;
    }

    public static final String ID_MODE = "detachMode";
    public static final String ID_KEYS = "keys";

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, DetachStrategy.class.getCanonicalName());
        map.put(ID_MODE, this.detachMode.toString());
        map.put(ID_KEYS, this.keys);
        return new MapConfiguration(map);
    }

    public enum DetachMode {

        ALL,
        CUSTOM,
        NONE;
    }

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder {

        private DetachMode detachMode = DetachMode.NONE;
        private final Set<String> keys = new HashSet<>();

        Builder() {}

        public Builder detachMode(final DetachMode detachMode) {
            this.detachMode = detachMode;
            return this;
        }

        public Builder detachMode(final String detachMode) {
            return detachMode(DetachMode.valueOf(detachMode));
        }

        public Builder keys(final String key, final String... rest) {
            keys.clear();
            keys.add(key);
            keys.addAll(Arrays.asList(rest));
            return this;
        }

        public Builder keys(final Collection<String> keys) {
            keys.clear();
            keys.addAll(keys);
            return this;
        }

        public DetachStrategy create() {
            return new DetachStrategy(this);
        }
    }

    public static class DetachOptions {
        private DetachMode detachMode = DetachMode.ALL;;
        private Set<String> keys;

        private DetachOptions() {}

        public DetachOptions(DetachMode detachMode, Collection<String> keys)
        {
            this.detachMode = detachMode;
            this.keys = new HashSet<>(keys);
        }

        public DetachMode getDetachMode() {
            return this.detachMode;
        }

        public Set<String> getProperties() {
            return this.keys;
        }
    }

    public static class DetachElementStep<S, E> extends ScalarMapStep<S, E> {

        private DetachOptions detachOptions;

        public DetachElementStep(final Traversal.Admin traversal) {

            super(traversal);

            detachOptions = new DetachOptions();
        }

        public DetachElementStep(final Traversal.Admin traversal, final DetachOptions detachOptions) {

            super(traversal);
            this.detachOptions = detachOptions;
        }

        @Override
        protected E map(final Traverser.Admin<S> traverser) {
            return DetachedFactory.detach(traverser.get(), detachOptions);
        }
    }
}
