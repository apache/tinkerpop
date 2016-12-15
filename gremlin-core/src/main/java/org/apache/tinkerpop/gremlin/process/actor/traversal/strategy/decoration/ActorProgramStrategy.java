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
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.actor.Actors;
import org.apache.tinkerpop.gremlin.process.actor.GraphActors;
import org.apache.tinkerpop.gremlin.process.actor.traversal.step.map.TraversalActorProgramStep;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.structure.util.partitioner.HashPartitioner;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ActorProgramStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
        implements TraversalStrategy.DecorationStrategy {

    private static final Set<Class<? extends DecorationStrategy>> PRIORS = Collections.singleton(RemoteStrategy.class);

    private final Actors actors;

    private ActorProgramStrategy() {
        this(null);
    }

    public ActorProgramStrategy(final Actors actors) {
        this.actors = actors;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        ReadOnlyStrategy.instance().apply(traversal);

        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        final TraversalActorProgramStep<?, ?> actorStep = new TraversalActorProgramStep<>(traversal, this.actors.getGraphActorsClass(),
                1 == this.actors.getWorkers() ?
                        traversal.getGraph().orElse(EmptyGraph.instance()).partitioner() :
                        new HashPartitioner(traversal.getGraph().orElse(EmptyGraph.instance()).partitioner(), this.actors.getWorkers()));
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

    public static final String GRAPH_ACTORS = "graphActors";
    public static final String WORKERS = "workers";

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(GRAPH_ACTORS, this.actors.getGraphActorsClass().getCanonicalName());
        map.put(WORKERS, this.actors.getWorkers());
        return new MapConfiguration(map);
    }

    public static ActorProgramStrategy create(final Configuration configuration) {
        try {
            final ActorProgramStrategy.Builder builder = ActorProgramStrategy.build();
            for (final String key : (List<String>) IteratorUtils.asList(configuration.getKeys())) {
                if (key.equals(GRAPH_ACTORS))
                    builder.graphComputer((Class) Class.forName(configuration.getString(key)));
                else if (key.equals(WORKERS))
                    builder.workers(configuration.getInt(key));
                else
                    throw new IllegalArgumentException("The provided key is unknown: " + key);
            }
            return builder.create();
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public static ActorProgramStrategy.Builder build() {
        return new ActorProgramStrategy.Builder();
    }

    public final static class Builder {

        private Actors actors = Actors.of(GraphActors.class);

        private Builder() {
        }

        public Builder computer(final Actors actors) {
            this.actors = actors;
            return this;
        }

        public Builder graphComputer(final Class<? extends GraphActors> graphActorsClass) {
            this.actors = this.actors.graphActors(graphActorsClass);
            return this;
        }


        public Builder workers(final int workers) {
            this.actors = this.actors.workers(workers);
            return this;
        }

        public ActorProgramStrategy create() {
            return new ActorProgramStrategy(this.actors);
        }
    }

}

