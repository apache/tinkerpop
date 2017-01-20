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

package org.apache.tinkerpop.gremlin.process.actors.traversal;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.actors.Actor;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectSetMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.StartMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.Terminate;
import org.apache.tinkerpop.gremlin.process.actors.traversal.strategy.decoration.ActorProgramStrategy;
import org.apache.tinkerpop.gremlin.process.actors.traversal.strategy.verification.ActorVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalActorProgram<R> implements ActorProgram {

    public static boolean DETACH = true;

    public static final String TRAVERSAL_ACTOR_PROGRAM_BYTECODE = "gremlin.traversalActorProgram.bytecode";

    private static final List<Class> MESSAGE_PRIORITIES = Arrays.asList(
            StartMessage.class,
            Traverser.class,
            SideEffectAddMessage.class,
            BarrierAddMessage.class,
            SideEffectSetMessage.class,
            BarrierDoneMessage.class,
            Terminate.class);

    private Traversal.Admin<?, R> traversal;
    public TraverserSet<R> result = new TraverserSet<>();

    public TraversalActorProgram() {

    }

    public TraversalActorProgram(final Traversal.Admin<?, R> traversal) {
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
    public void storeState(final Configuration configuration) {
        configuration.setProperty(ACTOR_PROGRAM, TraversalActorProgram.class.getCanonicalName());
        configuration.setProperty(TRAVERSAL_ACTOR_PROGRAM_BYTECODE, this.traversal.getBytecode());
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        final Bytecode bytecode = (Bytecode) configuration.getProperty(TRAVERSAL_ACTOR_PROGRAM_BYTECODE);
        this.traversal = (Traversal.Admin<?, R>) JavaTranslator.of(graph.traversal()).translate(bytecode);
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
    public TraversalActorProgram.Worker createWorkerProgram(final Actor.Worker worker) {
        return new TraversalWorkerProgram(worker, this.traversal.clone());
    }

    @Override
    public TraversalActorProgram.Master createMasterProgram(final Actor.Master master) {
        return new TraversalMasterProgram(master, this.traversal.clone(), this.result);
    }

    @Override
    public Optional<List<Class>> getMessagePriorities() {
        return Optional.of(MESSAGE_PRIORITIES);
    }

    @Override
    public TraversalActorProgram<R> clone() {
        try {
            final TraversalActorProgram<R> clone = (TraversalActorProgram<R>) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static <A> A attach(final A object, final Host host) {
        if (DETACH) {
            if (object instanceof Map) {
                final Map map = (Map) object;
                for (final Object key : map.keySet()) {
                    map.put(TraversalActorProgram.attach(key, host), TraversalActorProgram.attach(map.get(key), host));
                }
                return (A) map;
            } else if (object instanceof List) {
                final List list = (List) object;
                for (int i = 0; i < list.size(); i++) {
                    list.set(i, TraversalActorProgram.attach(list.get(i), host));
                }
                return (A) list;
            } else if (object instanceof Map.Entry) {
                final Map.Entry entry = (Map.Entry) object;
                entry.setValue(TraversalActorProgram.attach(entry.getValue(), host));
                return (A) entry;
            } else if (object instanceof TraverserSet) {
                ((TraverserSet<?>) object).forEach(traverser -> TraversalActorProgram.attach(traverser, host));
                return object;
            } else if (object instanceof BulkSet) {
                final BulkSet<?> set = (BulkSet) object;
                final BulkSet newSet = new BulkSet();
                set.forEach((o, b) -> newSet.add(TraversalActorProgram.attach(o, host), b));
                return (A) newSet;
            } else if (object instanceof Set) {
                final Set set = (Set) object;
                final Set newSet = set instanceof HashSet ? new HashSet<>(set.size()) : new LinkedHashSet<>(set.size());
                set.forEach(o -> newSet.add(TraversalActorProgram.attach(o, host)));
                return (A) newSet;
            } else if (object instanceof Traverser.Admin) {
                final Traverser.Admin traverser = (Traverser.Admin) object;
                traverser.attach(host);
                traverser.set(TraversalActorProgram.attach(traverser.get(), host));
                return (A) traverser;
            } else if (object instanceof Attachable) {
                return (A) ((Attachable) object).attach(Attachable.Method.get(host instanceof Partition ? ((Partition) host).partitioner().getGraph() : host));
            } else {
                return host.attach(object).orElse(object);
            }
        } else
            return object;
    }
}