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

package org.apache.tinkerpop.gremlin.akka.process.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.StartMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.VoteToHaltMessage;
import org.apache.tinkerpop.gremlin.akka.process.traversal.strategy.verification.ActorVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MasterTraversalActor extends AbstractActor implements RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private final Traversal.Admin<?, ?> traversal;
    private final TraversalMatrix<?, ?> matrix;
    private final Partitioner partitioner;
    private final Map<String, ActorSelection> workers = new HashMap<>();
    private Map<String, Barrier> barriers = new HashMap<>();
    private final TraverserSet<?> results;
    private final String leaderWorker;

    public MasterTraversalActor(final Traversal.Admin<?, ?> traversal, final Partitioner partitioner, final TraverserSet<?> results) {
        System.out.println("master[created]: " + self().path());
        final TraversalStrategies strategies = traversal.getStrategies().clone();
        strategies.removeStrategies(ComputerVerificationStrategy.class, StandardVerificationStrategy.class);
        strategies.addStrategies(ActorVerificationStrategy.instance());
        traversal.setStrategies(strategies);
        traversal.applyStrategies();

        this.traversal = ((TraversalVertexProgramStep) traversal.getStartStep()).computerTraversal.get();
        System.out.println(this.traversal);
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.partitioner = partitioner;
        this.results = results;
        this.initializeWorkers();
        this.leaderWorker = "worker-" + this.partitioner.getPartitions().get(0).hashCode();

        receive(ReceiveBuilder.
                match(Traverser.Admin.class, traverser -> {
                    this.processTraverser(traverser);
                }).
                match(BarrierAddMessage.class, barrierMerge -> {
                    // get the barrier updates from the workers to synchronize against the master barrier
                    final Barrier barrier = (Barrier) this.matrix.getStepById(barrierMerge.getStepId());
                    final Step<?, ?> step = (Step) barrier;
                    GraphComputing.atMaster(step, true);
                    barrier.addBarrier(barrierMerge.getBarrier());
                    this.barriers.put(step.getId(), barrier);
                }).
                match(SideEffectAddMessage.class, sideEffect -> {
                    // get the side-effect updates from the workers to generate the master side-effects
                    this.traversal.getSideEffects().add(sideEffect.getSideEffectKey(), sideEffect.getSideEffectValue());
                }).
                match(VoteToHaltMessage.class, voteToHalt -> {
                    assert !sender().equals(self());
                    if (!this.barriers.isEmpty()) {
                        for (final Barrier barrier : this.barriers.values()) {
                            final Step<?, ?> step = (Step) barrier;
                            if (!(barrier instanceof LocalBarrier)) {
                                while (step.hasNext()) {
                                    this.sendTraverser(step.next());
                                }
                            } else {
                                this.traversal.getSideEffects().forEach((k, v) -> {
                                    this.broadcast(new SideEffectAddMessage(k, v));
                                });
                                this.broadcast(new BarrierDoneMessage(barrier));
                                barrier.done();
                            }
                        }
                        this.barriers.clear();
                        worker(this.leaderWorker).tell(StartMessage.instance(), self());
                    } else {
                        while (this.traversal.hasNext()) {
                            this.results.add((Traverser.Admin) this.traversal.nextTraverser());
                        }
                        context().system().terminate();
                    }
                }).build());
    }

    private void initializeWorkers() {
        final List<Partition> partitions = this.partitioner.getPartitions();
        for (final Partition partition : partitions) {
            final String workerPathString = "worker-" + partition.hashCode();
            final ActorRef worker = context().actorOf(Props.create(WorkerTraversalActor.class, this.traversal.clone(), partition, this.partitioner), workerPathString);
            this.workers.put(workerPathString, context().actorSelection(worker.path()));
        }
        for (final ActorSelection worker : this.workers.values()) {
            worker.tell(StartMessage.instance(), self());
        }
        this.workers.clear();
    }

    private void broadcast(final Object message) {
        for (final Partition partition : this.partitioner.getPartitions()) {
            worker("worker-" + partition.hashCode()).tell(message, self());
        }
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted() || traverser.get() instanceof Element) {
            this.sendTraverser(traverser);
        } else {
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            GraphComputing.atMaster(step, true);
            step.addStart(traverser);
            while (step.hasNext()) {
                this.processTraverser(step.next());
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted())
            this.results.add(traverser);
        else if (traverser.get() instanceof Element)
            worker("worker-" + this.partitioner.getPartition((Element) traverser.get()).hashCode()).tell(traverser, self());
        else
            self().tell(traverser, self());
    }

    private ActorSelection worker(final String workerPath) {
        ActorSelection worker = this.workers.get(workerPath);
        if (null == worker) {
            worker = context().actorSelection(workerPath);
            this.workers.put(workerPath, worker);
        }
        return worker;
    }
}
