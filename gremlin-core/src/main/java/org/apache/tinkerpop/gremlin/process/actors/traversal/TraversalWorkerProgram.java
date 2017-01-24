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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalWorkerProgram implements ActorProgram.Worker<Object> {

    private final Actor.Worker self;
    private final TraversalMatrix<?, ?> matrix;
    private final Map<Partition, Address.Worker> partitionToWorkerMap = new HashMap<>();
    //
    private Address neighborAddress;
    private boolean voteToHalt = true;
    private Map<String, Barrier> barriers = new HashMap<>();

    public TraversalWorkerProgram(final Actor.Worker self, final Traversal.Admin<?, ?> traversal) {
        this.self = self;
        final WorkerTraversalSideEffects sideEffects = new WorkerTraversalSideEffects(traversal.getSideEffects(), this.self);
        TraversalHelper.applyTraversalRecursively(t -> t.setSideEffects(sideEffects), traversal);
        this.matrix = new TraversalMatrix<>(traversal);
        Distributing.configure(traversal, false, true);
        Pushing.configure(traversal, true, false);
    }

    @Override
    public void setup() {
        // create termination ring topology
        final int i = this.self.workers().indexOf(this.self.address());
        this.neighborAddress = i == this.self.workers().size() - 1 ? this.self.master() : this.self.workers().get(i + 1);
        for (int j = 0; j < this.self.partition().partitioner().getPartitions().size(); j++) {
            this.partitionToWorkerMap.put(this.self.partition().partitioner().getPartitions().get(j), this.self.workers().get(j));
        }
        // configure all the GraphSteps to be partition-centric
        TraversalHelper.getStepsOfAssignableClassRecursively(GraphStep.class, this.matrix.getTraversal()).forEach(graphStep -> {
            if (0 == graphStep.getIds().length)
                graphStep.setIteratorSupplier(graphStep.returnsVertex() ?
                        this.self.partition()::vertices :
                        this.self.partition()::edges);
            else {
                graphStep.setIteratorSupplier(graphStep.returnsVertex() ?
                        () -> IteratorUtils.filter(self.partition().vertices(graphStep.getIds()), this.self.partition()::contains) :
                        () -> IteratorUtils.filter(self.partition().edges(graphStep.getIds()), this.self.partition()::contains));
            }
        });
    }

    @Override
    public void execute(final Object message) {
        if (message instanceof StartMessage) {
            // initial message from master that says: "start processing"
            final GraphStep<?, ?> step = (GraphStep) this.matrix.getTraversal().getStartStep();
            while (step.hasNext()) {
                this.sendTraverser(step.next());
            }
        } else if (message instanceof Traverser.Admin) {
            this.processTraverser((Traverser.Admin) message);
        } else if (message instanceof SideEffectSetMessage) {
            this.matrix.getTraversal().getSideEffects().
                    set(((SideEffectSetMessage) message).getKey(), TraversalActorProgram.attach(((SideEffectSetMessage) message).getValue(), this.self.partition()));
        } else if (message instanceof BarrierDoneMessage) {
            final Step<?, ?> step = (Step) this.matrix.getStepById(((BarrierDoneMessage) message).getStepId());
            if (step instanceof LocalBarrier) { // the worker drains the local barrier
                while (step.hasNext()) {
                    sendTraverser(step.next());
                }
            } else
                ((Barrier) step).done();       // the master drains the global barrier
        } else if (message instanceof Terminate) {
            final Terminate terminate = (Terminate) message;
            if (this.voteToHalt && !this.barriers.isEmpty()) {
                for (final Barrier barrier : this.barriers.values()) {
                    if (barrier instanceof LocalBarrier) {
                        barrier.processAllStarts();
                        this.self.send(this.self.master(), new BarrierAddMessage(barrier));
                    } else {
                        while (barrier.hasNextBarrier()) {
                            this.self.send(this.self.master(), new BarrierAddMessage(barrier));
                        }
                    }
                }
                this.barriers.clear();
                this.voteToHalt = false;
            }
            // use termination token to determine termination condition
            this.self.send(this.neighborAddress, this.voteToHalt ? terminate : Terminate.NO);
            this.voteToHalt = true;
        } else {
            throw new IllegalArgumentException("The following message is unknown: " + message);
        }
    }

    @Override
    public void terminate() {

    }

    //////////////

    private void processTraverser(final Traverser.Admin traverser) {
        assert !(traverser.get() instanceof Element) || this.self.partition().contains((Element) traverser.get());
        if (traverser.isHalted())
            this.sendTraverser(traverser);
        else {
            TraversalActorProgram.attach(traverser, this.self.partition());
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            if (step instanceof Barrier) {
                this.barriers.put(step.getId(), (Barrier) step);
            } else {
                while (step.hasNext()) {
                    this.sendTraverser(step.next());
                }
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        this.voteToHalt = false;
        this.detachTraverser(traverser);
        if (traverser.isHalted())
            this.self.send(this.self.master(), traverser);
        else if (traverser.get() instanceof Element && !this.self.partition().contains((Element) traverser.get()))
            this.self.send(this.partitionToWorkerMap.get(this.self.partition().partitioner().find((Element) traverser.get())), traverser);
        else
            this.self.send(this.self.address(), traverser);
    }

    private final Traverser.Admin detachTraverser(final Traverser.Admin traverser) {
        return TraversalActorProgram.DETACH ? traverser.detach() : traverser;
    }

}
