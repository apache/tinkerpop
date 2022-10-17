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

    private DetachOptions detachOptions;

    public DetachStrategy(final DetachMode detachMode, final String[] properties) {
        detachOptions = new DetachOptions();
        detachOptions.detachMode = detachMode;
        detachOptions.properties = properties;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() == EmptyStep.instance()) {
            final Optional<ProfileSideEffectStep> profileStep = TraversalHelper.getFirstStepOfAssignableClass(ProfileSideEffectStep.class, traversal);
            final int index = profileStep.map(step -> traversal.getSteps().indexOf(step))
                    .orElseGet(() -> traversal.getSteps().size());
            traversal.addStep(index, new DetachElementStep<>(traversal, detachOptions));
        }
    }

    public static final String ID_MODE = "mode";
    public static final String ID_PROPERTIES = "properties";

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STRATEGY, DetachStrategy.class.getCanonicalName());
        map.put(ID_MODE, this.detachOptions.detachMode);
        map.put(ID_PROPERTIES, this.detachOptions.properties);
        return new MapConfiguration(map);
    }

    public enum DetachMode {

        ALL,
        CUSTOM,
        NONE;
    }

    // todo: add builder
    public static class DetachOptions {
        public DetachMode detachMode;
        public String[] properties;
    }

    public static class DetachElementStep<S, E> extends ScalarMapStep<S, E> {

        private DetachOptions detachOptions;

        public DetachElementStep(final Traversal.Admin traversal) {

            super(traversal);

            detachOptions = new DetachOptions();
            detachOptions.detachMode = DetachMode.NONE;
        }

        public DetachElementStep(final Traversal.Admin traversal, final DetachOptions detachOptions) {

            super(traversal);
            this.detachOptions = detachOptions;
        }

        // todo: update factory
        @Override
        protected E map(final Traverser.Admin<S> traverser) {
            switch(detachOptions.detachMode) {
                case ALL:
                    return DetachedFactory.detach(traverser.get(), true);
                case NONE:
                    return DetachedFactory.detach(traverser.get(), false);
                default:
                    throw new UnsupportedOperationException("TODO");
            }
        }
    }
}
