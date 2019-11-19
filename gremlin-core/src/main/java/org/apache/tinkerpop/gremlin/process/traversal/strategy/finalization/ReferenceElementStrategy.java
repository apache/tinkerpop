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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.Optional;

/**
 * A strategy that detaches traversers with graph elements as references (i.e. without properties - just {@code id}
 * and {@code label}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ReferenceElementStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    private static final ReferenceElementStrategy INSTANCE = new ReferenceElementStrategy();

    private ReferenceElementStrategy() {}

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getParent() == EmptyStep.instance()) {
            final Optional<ProfileSideEffectStep> profileStep = TraversalHelper.getFirstStepOfAssignableClass(ProfileSideEffectStep.class, traversal);
            final int index = profileStep.map(step -> traversal.getSteps().indexOf(step))
                    .orElseGet(() -> traversal.getSteps().size());
            traversal.addStep(index, new ReferenceElementStep<>(traversal));
        }
    }

    public static ReferenceElementStrategy instance() {
        return INSTANCE;
    }

    public static class ReferenceElementStep<S, E> extends ScalarMapStep<S, E> {

        public ReferenceElementStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected E map(final Traverser.Admin<S> traverser) {
            return ReferenceFactory.detach(traverser.get());
        }
    }
}
