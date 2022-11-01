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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A strategy that manages the properties that will be in the result.
 *
 * @author Valentyn Kahamlyk
 */
public final class DetachStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final DetachStrategy INSTANCE = new DetachStrategy(DetachMode.ALL, null);
    private DetachMode detachMode = DetachMode.ALL;
    private String[] properties;

    private DetachStrategy() {
    }

    private DetachStrategy(final DetachMode detachMode, final String[] properties) {
        this.detachMode = detachMode;
        this.properties = properties;
    }

    private DetachStrategy(final Builder builder) {
        this.detachMode = builder.detachMode;
        this.properties = builder.properties;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() == EmptyStep.instance()) {
            final Optional<ProfileSideEffectStep> profileStep = TraversalHelper.getFirstStepOfAssignableClass(ProfileSideEffectStep.class, traversal);
            final int index = profileStep.map(step -> traversal.getSteps().indexOf(step))
                    .orElseGet(() -> traversal.getSteps().size());
            traversal.addStep(index,
                    new DetachElementStep<>(traversal, new DetachOptions(detachMode, properties)));
        }
    }

    public static DetachStrategy create(final Configuration configuration) {
        return new DetachStrategy(DetachMode.valueOf(configuration.getString(ID_MODE)), configuration.getStringArray(ID_PROPERTIES));
    }

    /**
     * Gets the standard configuration of this strategy that will return all properties.
     */
    public static DetachStrategy instance() {
        return INSTANCE;
    }

    public static final String ID_MODE = "detachMode";
    public static final String ID_PROPERTIES = "properties";

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, DetachStrategy.class.getCanonicalName());
        map.put(ID_MODE, this.detachMode.toString());
        map.put(ID_PROPERTIES, this.properties);
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
        private String[] properties;

        Builder() {}

        public Builder detachMode(final DetachMode detachMode) {
            this.detachMode = detachMode;
            return this;
        }

        public Builder detachMode(final String detachMode) {
            this.detachMode = DetachMode.valueOf(detachMode);
            return this;
        }

        public Builder properties(final String[] properties) {
            this.properties = properties;
            return this;
        }

        public DetachStrategy create() {
            return new DetachStrategy(this);
        }
    }

    public static class DetachOptions {
        private DetachMode detachMode = DetachMode.ALL;;
        private String[] properties;

        private DetachOptions() {}

        public DetachOptions(DetachMode detachMode, String[] properties)
        {
            this.detachMode = detachMode;
            this.properties = properties;
        }

        public DetachMode getDetachMode() {
            return this.detachMode;
        }

        public String[] getProperties() {
            return this.properties;
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
