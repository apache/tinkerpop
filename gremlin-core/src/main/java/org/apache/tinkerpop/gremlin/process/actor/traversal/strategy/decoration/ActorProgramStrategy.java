/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.decoration;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.actor.GraphActors;
import org.apache.tinkerpop.gremlin.process.actor.traversal.step.map.TraversalActorProgramStep;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.ProcessorTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy, ProcessorTraversalStrategy<GraphActors> {

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = Collections.singleton(RemoteStrategy.class);

    private final Configuration graphActorsConfiguration;

    public ActorProgramStrategy(final GraphActors graphActors) {
        this.graphActorsConfiguration = graphActors.configuration();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        ReadOnlyStrategy.instance().apply(traversal);

        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        final TraversalActorProgramStep<?, ?> actorStep = new TraversalActorProgramStep<>(traversal, this.graphActorsConfiguration);
        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(actorStep);

        // validations
        assert traversal.getStartStep().equals(actorStep);
        assert traversal.getSteps().size() == 1;
        assert traversal.getEndStep() == actorStep;
    }

    @Override
    public Set<Class<? extends DecorationStrategy>> applyPrior() {
        return PRIORS;
    }

    ////////////////////////////////////////////////////////////

    @Override
    public Configuration getConfiguration() {
        return this.graphActorsConfiguration;
    }

    public static ActorProgramStrategy create(final Configuration configuration) {
        try {
            return new ActorProgramStrategy(GraphActors.open(configuration));
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public GraphActors getProcessor() {
        return GraphActors.open(this.graphActorsConfiguration);
    }

}

