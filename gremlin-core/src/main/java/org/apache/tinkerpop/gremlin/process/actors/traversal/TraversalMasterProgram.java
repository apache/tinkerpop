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

import org.apache.tinkerpop.gremlin.process.actors.Actor;
import org.apache.tinkerpop.gremlin.process.actors.ActorProgram;
import org.apache.tinkerpop.gremlin.process.actors.Address;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.SideEffectSetMessage;
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.Terminate;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Distributing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Pushing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.OrderedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalMasterProgram<R> implements ActorProgram.Master<Object> {

    private final Actor.Master<Pair<TraverserSet<R>, Map<String, Object>>> master;
    private final Traversal.Admin<?, R> traversal;
    private final TraversalMatrix<?, R> matrix;
    private Map<String, Barrier> barriers = new HashMap<>();
    private Set<String> sideEffects = new HashSet<>();
    private final TraverserSet<R> traverserResults;
    private Address.Worker neighborAddress;
    private int orderCounter = -1;
    private final Map<Partition, Address.Worker> partitionToWorkerMap = new HashMap<>();
    private boolean voteToHalt = true;
    private boolean barriersDone = false;

    public TraversalMasterProgram(final Actor.Master<Pair<TraverserSet<R>, Map<String, Object>>> master, final Traversal.Admin<?, R> traversal) {
        this.traversal = traversal;
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.traverserResults = new TraverserSet<>();
        this.master = master;
        Distributing.configure(this.traversal, true, true);
        Pushing.configure(this.traversal, true, false);
    }

    @Override
    public void setup() {
        // create termination ring
        this.neighborAddress = this.master.workers().get(0);
        // create worker lookup table
        for (int i = 0; i < this.master.partitioner().getPartitions().size(); i++) {
            this.partitionToWorkerMap.put(this.master.partitioner().getPartitions().get(i), this.master.workers().get(i));
        }
        // first pass of a two pass termination detection
        this.voteToHalt = false;
        this.master.send(this.neighborAddress, Terminate.NO);
    }

    @Override
    public void execute(final Object message) {
        if (message instanceof Traverser.Admin) {
            final Traverser.Admin traverser = (Traverser.Admin) message;
            if (traverser.isHalted() || traverser.get() instanceof Element) {
                this.sendTraverser(traverser);
            } else {
                // attach traverser for local processing at the master actor
                TraversalActorProgram.attach(traverser, this.master.partitioner().getGraph());
                final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
                step.addStart(traverser);
                if (step instanceof Barrier) {
                    this.barriers.put(step.getId(), (Barrier) step);
                    this.barriersDone = false;
                } else {
                    while (step.hasNext()) {
                        this.sendTraverser(step.next());
                    }
                }
            }
        } else if (message instanceof BarrierAddMessage) {
            final Barrier barrier = (Barrier) this.matrix.getStepById(((BarrierAddMessage) message).getStepId());
            if (!(barrier instanceof LocalBarrier))
                barrier.addBarrier(TraversalActorProgram.attach(((BarrierAddMessage) message).getBarrier(), this.master.partitioner().getGraph()));
            if (barrier instanceof SideEffectCapable)
                this.sideEffects.add(((SideEffectCapable) barrier).getSideEffectKey());
            if (barrier instanceof SideEffectCapStep)
                this.sideEffects.addAll(((SideEffectCapStep) barrier).getSideEffectKeys());
            this.barriers.put(((Step) barrier).getId(), barrier);
            this.barriersDone = false;
        } else if (message instanceof SideEffectAddMessage) {
            final SideEffectAddMessage sideEffectAddMessage = (SideEffectAddMessage) message;
            this.traversal.getSideEffects().add(sideEffectAddMessage.getKey(), TraversalActorProgram.attach(sideEffectAddMessage.getValue(), this.master.partitioner().getGraph()));
            this.sideEffects.add(sideEffectAddMessage.getKey());
        } else if (message instanceof Terminate) {
            if (message == Terminate.NO)
                this.voteToHalt = false;
            if (this.voteToHalt && !this.sideEffects.isEmpty()) {
                // process all side-effect updates
                for (final String key : this.sideEffects) {
                    this.broadcast(new SideEffectSetMessage(key, this.traversal.getSideEffects().get(key)));
                }
                this.sideEffects.clear();
                this.voteToHalt = false;
                this.master.send(this.neighborAddress, Terminate.NO);
            } else if (this.voteToHalt && !this.barriers.isEmpty()) {
                if (!this.barriersDone) {
                    // tell all workers that the barrier is complete
                    for (final Barrier barrier : this.barriers.values()) {
                        this.broadcast(new BarrierDoneMessage(barrier));
                    }
                    this.barriersDone = true;
                } else {
                    // process all barriers
                    for (final Barrier barrier : this.barriers.values()) {
                        final Step<?, ?> step = (Step) barrier;
                        if (barrier instanceof LocalBarrier)  // the barriers are distributed amongst the workers
                            barrier.done();
                        else {                                // the barrier is at the master
                            this.orderBarrier(step);
                            if (step instanceof OrderGlobalStep) this.orderCounter = 0;
                            while (step.hasNext()) {
                                this.sendTraverser(-1 == this.orderCounter ?
                                        step.next() :
                                        new OrderedTraverser<>(step.next(), this.orderCounter++));
                            }
                        }
                    }
                    this.barriers.clear();
                    this.barriersDone = false;
                }
                this.voteToHalt = false;
                this.master.send(this.neighborAddress, Terminate.NO);
            } else if (!this.voteToHalt) {
                this.voteToHalt = true;
                this.master.send(this.neighborAddress, Terminate.YES);
            } else {
                // get any dangling local results (e.g. workers have no data but a reducing barrier is waiting for data)
                while (this.traversal.hasNext()) {
                    final Traverser.Admin traverser = this.traversal.nextTraverser();
                    this.traverserResults.add(-1 == this.orderCounter ? traverser : new OrderedTraverser(traverser, this.orderCounter++));
                }
                // if there is an ordering, order the result set
                if (this.orderCounter != -1)
                    this.traverserResults.sort((a, b) -> Integer.compare(((OrderedTraverser<?>) a).order(), ((OrderedTraverser<?>) b).order()));
                // generate the final result to send back to the GraphActors program
                final Map<String, Object> sideEffects = new HashMap<>();
                for (final String key : this.traversal.getSideEffects().keys()) {
                    sideEffects.put(key, this.traversal.getSideEffects().get(key));
                }
                // set the result (traversers and side-effects) to return to user
                this.master.setResult(Pair.with(this.traverserResults, sideEffects));
                // close master (and cascade close all workers)
                this.master.close();
            }
        } else {
            throw new IllegalStateException("Unknown message:" + message);
        }
    }

    @Override
    public void terminate() {

    }

    private void broadcast(final Object message) {
        for (final Address.Worker worker : this.master.workers()) {
            this.master.send(worker, message);
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted()) {
            TraversalActorProgram.attach(traverser, this.master.partitioner().getGraph());
            this.traverserResults.add(traverser);
            return;
        }
        //////
        this.voteToHalt = false;
        if (this.matrix.getStepById(traverser.getStepId()) instanceof GraphStep) {
            // mid-traversal V()/E() traversers need to be broadcasted across all workers/partitions
            for (final Address.Worker worker : this.master.workers()) {
                this.master.send(worker, traverser);
            }
        } else if (traverser.get() instanceof Element) {
            // if the traverser references an element of the graph, send it to respect data-local worker
            this.master.send(this.partitionToWorkerMap.get(this.master.partitioner().find((Element) traverser.get())), this.detachTraverser(traverser));
        } else {
            // if the traverser references a non-element, process it locally at the master // TODO: hash() distribute to load balance across workers
            this.master.send(this.master.address(), this.detachTraverser(traverser));
        }
    }

    private void orderBarrier(final Step step) {
        if (this.orderCounter != -1 && step instanceof Barrier && (step instanceof RangeGlobalStep || step instanceof TailGlobalStep)) {
            final Barrier barrier = (Barrier) step;
            final TraverserSet<?> rangingBarrier = (TraverserSet<?>) barrier.nextBarrier();
            rangingBarrier.sort((a, b) -> Integer.compare(((OrderedTraverser<?>) a).order(), ((OrderedTraverser<?>) b).order()));
            barrier.addBarrier(rangingBarrier);
        }
    }

    private final Traverser.Admin detachTraverser(final Traverser.Admin traverser) {
        return TraversalActorProgram.DETACH ? traverser.detach() : traverser;
    }

}
