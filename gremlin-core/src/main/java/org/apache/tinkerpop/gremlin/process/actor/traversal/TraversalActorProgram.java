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

package org.apache.tinkerpop.gremlin.process.actor.traversal;

import org.apache.tinkerpop.gremlin.process.actor.Actor;
import org.apache.tinkerpop.gremlin.process.actor.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actor.ActorsResult;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.SideEffectSetMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.StartMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.Terminate;
import org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.decoration.ActorProgramStrategy;
import org.apache.tinkerpop.gremlin.process.actor.traversal.strategy.verification.ActorVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalActorProgram<R> implements ActorProgram<TraverserSet<R>> {

    private static final List<Class> MESSAGE_PRIORITIES = Arrays.asList(
            StartMessage.class,
            Traverser.class,
            SideEffectAddMessage.class,
            BarrierAddMessage.class,
            SideEffectSetMessage.class,
            BarrierDoneMessage.class,
            Terminate.class);

    private Traversal.Admin<?, R> traversal;
    private final Partitioner partitioner;
    public TraverserSet<R> result = new TraverserSet<>();

    public TraversalActorProgram(final Traversal.Admin<?, R> traversal, final Partitioner partitioner) {
        this.partitioner = partitioner;
        this.traversal = traversal;
        final TraversalStrategies strategies = this.traversal.getStrategies().clone();
        strategies.addStrategies(ActorVerificationStrategy.instance(), ReadOnlyStrategy.instance());
        // TODO: make TinkerGraph/etc. strategies smart about actors
        new ArrayList<>(strategies.toList()).stream().
                filter(s -> s instanceof TraversalStrategy.ProviderOptimizationStrategy).
                map(TraversalStrategy::getClass).
                forEach(strategies::removeStrategies);
        strategies.removeStrategies(
                ActorProgramStrategy.class,
                LazyBarrierStrategy.class,
                RepeatUnrollStrategy.class,
                MatchPredicateStrategy.class,
                InlineFilterStrategy.class,
                PathRetractionStrategy.class);
        this.traversal.setStrategies(strategies);
        this.traversal.applyStrategies();
    }

    @Override
    public Worker createWorkerProgram(final Actor.Worker worker) {
        return new TraversalWorkerProgram<>(worker, this.traversal.clone(), this.partitioner);
    }

    @Override
    public Master createMasterProgram(final Actor.Master master) {
        return new TraversalMasterProgram<>(master, this.traversal.clone(), this.partitioner, this.result);
    }

    @Override
    public Optional<List<Class>> getMessagePriorities() {
        return Optional.of(MESSAGE_PRIORITIES);
    }

    @Override
    public ActorProgram<TraverserSet<R>> clone() {
        try {
            final TraversalActorProgram<R> clone = (TraversalActorProgram<R>) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}