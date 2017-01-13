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
import org.apache.tinkerpop.gremlin.process.actors.traversal.message.StartMessage;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.OrderedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalMasterProgram implements ActorProgram.Master<Object> {

    private final Actor.Master master;
    private final Traversal.Admin<?, ?> traversal;
    private final TraversalMatrix<?, ?> matrix;
    private Map<String, Barrier> barriers = new HashMap<>();
    private final TraverserSet<?> results;
    private Address.Worker leaderWorker;
    private int orderCounter = -1;
    private final Map<Partition, Address.Worker> partitionToWorkerMap = new HashMap<>();

    public TraversalMasterProgram(final Actor.Master master, final Traversal.Admin<?, ?> traversal, final TraverserSet<?> results) {
        this.traversal = traversal;
        // System.out.println("master[created]: " + master.address().getId());
        // System.out.println(this.traversal);
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.results = results;
        this.master = master;
        Distributing.configure(this.traversal, true, true);
        Pushing.configure(this.traversal, true, false);
    }

    @Override
    public void setup() {
        this.leaderWorker = this.master.workers().get(0);
        for (int i = 0; i < this.master.partitioner().getPartitions().size(); i++) {
            this.partitionToWorkerMap.put(this.master.partitioner().getPartitions().get(i), this.master.workers().get(i));
        }
        this.broadcast(StartMessage.instance());
        this.master.send(this.leaderWorker, Terminate.MAYBE);
    }

    @Override
    public void execute(final Object message) {
        if (message instanceof Traverser.Admin) {
            this.processTraverser((Traverser.Admin) message);
        } else if (message instanceof BarrierAddMessage) {
            final Barrier barrier = (Barrier) this.matrix.getStepById(((BarrierAddMessage) message).getStepId());
            final Step<?, ?> step = (Step) barrier;
            barrier.addBarrier(((BarrierAddMessage) message).getBarrier());
            this.barriers.put(step.getId(), barrier);
        } else if (message instanceof SideEffectAddMessage) {
            this.traversal.getSideEffects().add(((SideEffectAddMessage) message).getKey(), ((SideEffectAddMessage) message).getValue());
        } else if (message instanceof Terminate) {
            assert Terminate.YES == message;
            if (!this.barriers.isEmpty()) {
                for (final Barrier barrier : this.barriers.values()) {
                    final Step<?, ?> step = (Step) barrier;
                    if (!(barrier instanceof LocalBarrier)) {
                        this.orderBarrier(step);
                        if (step instanceof OrderGlobalStep) this.orderCounter = 0;
                        while (step.hasNext()) {
                            this.sendTraverser(-1 == this.orderCounter ?
                                    step.next() :
                                    new OrderedTraverser<>(step.next(), this.orderCounter++));
                        }
                    } else {
                        if (step instanceof SideEffectCapable) {
                            final String key = ((SideEffectCapable) step).getSideEffectKey();
                            this.broadcast(new SideEffectSetMessage(key, this.traversal.getSideEffects().get(key)));
                        }
                        this.broadcast(new BarrierDoneMessage(barrier));
                        barrier.done();
                    }
                }
                this.barriers.clear();
                this.master.send(this.leaderWorker, Terminate.MAYBE);
            } else {
                while (this.traversal.hasNext()) {
                    this.results.add((Traverser.Admin) this.traversal.nextTraverser());
                }
                if (this.orderCounter != -1)
                    this.results.sort((a, b) -> Integer.compare(((OrderedTraverser<?>) a).order(), ((OrderedTraverser<?>) b).order()));

                this.results.forEach(this::attachTraverser);
                this.master.close();
            }
        } else {
            throw new IllegalStateException("Unknown message:" + message);
        }
    }

    @Override
    public void terminate() {
        this.master.result().setResult(this.results);
    }

    private void broadcast(final Object message) {
        for (final Address.Worker worker : this.master.workers()) {
            this.master.send(worker, message);
        }
    }

    private void processTraverser(final Traverser.Admin traverser) {
        this.attachTraverser(traverser);
        if (traverser.isHalted() || traverser.get() instanceof Element) {
            this.sendTraverser(traverser);
        } else {
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            if (step instanceof Barrier) {
                this.barriers.put(step.getId(), (Barrier) step);
            } else {
                while (step.hasNext()) {
                    this.processTraverser(step.next());
                }
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted())
            this.results.add(traverser);
        else if (traverser.get() instanceof Element)
            this.master.send(this.partitionToWorkerMap.get(this.master.partitioner().find((Element) traverser.get())), this.detachTraverser(traverser));
        else
            this.master.send(this.master.address(), this.detachTraverser(traverser));
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

    private void attachTraverser(final Traverser.Admin traverser) {
        if (TraversalActorProgram.DETACH && traverser.get() instanceof Element)
            traverser.attach(this.master.partitioner().find((Element) traverser.get()));
    }
}
