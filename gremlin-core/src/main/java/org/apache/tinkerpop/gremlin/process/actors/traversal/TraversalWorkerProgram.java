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

    private final Actor.Worker worker;
    private final TraversalMatrix<?, ?> matrix;
    private final Map<Partition, Address.Worker> partitionToWorkerMap = new HashMap<>();
    //
    private Address neighborAddress;
    private boolean voteToHalt = true;
    private Map<String, Barrier> barriers = new HashMap<>();

    public TraversalWorkerProgram(final Actor.Worker worker, final Traversal.Admin<?, ?> traversal) {
        this.worker = worker;
        // create a pass-through side-effects which sends SideEffectAddMessages to the master actor
        final WorkerTraversalSideEffects sideEffects = new WorkerTraversalSideEffects(traversal.getSideEffects(), this.worker);
        TraversalHelper.applyTraversalRecursively(t -> t.setSideEffects(sideEffects), traversal);
        this.matrix = new TraversalMatrix<>(traversal);
        // configure distributing and pushing semantics for worker execution
        Distributing.configure(traversal, false, true);
        Pushing.configure(traversal, true, false);
        // configure all the GraphSteps to be partition-centric (TODO: GraphStep should implement distributing and be smart to get the partition from the traversal)
        TraversalHelper.getStepsOfAssignableClassRecursively(GraphStep.class, this.matrix.getTraversal()).forEach(graphStep -> {
            if (0 == graphStep.getIds().length)
                graphStep.setIteratorSupplier(graphStep.returnsVertex() ?
                        this.worker.partition()::vertices :
                        this.worker.partition()::edges);
            else {
                graphStep.setIteratorSupplier(graphStep.returnsVertex() ?
                        () -> IteratorUtils.filter(worker.partition().vertices(graphStep.getIds()), this.worker.partition()::contains) :
                        () -> IteratorUtils.filter(worker.partition().edges(graphStep.getIds()), this.worker.partition()::contains));
            }
        });
    }

    @Override
    public void setup() {
        // create termination ring topology
        final int i = this.worker.workers().indexOf(this.worker.address());
        this.neighborAddress = i == this.worker.workers().size() - 1 ? this.worker.master() : this.worker.workers().get(i + 1);
        for (int j = 0; j < this.worker.partition().partitioner().getPartitions().size(); j++) {
            this.partitionToWorkerMap.put(this.worker.partition().partitioner().getPartitions().get(j), this.worker.workers().get(j));
        }
        // once loaded, start processing start step
        final Step<?, ?> step = this.matrix.getTraversal().getStartStep();
        while (step.hasNext()) {
            this.sendTraverser(step.next());
        }
    }

    @Override
    public void execute(final Object message) {
        if (message instanceof Traverser.Admin) {
            ////////// PROCESS TRAVERSER //////////
            final Traverser.Admin traverser = (Traverser.Admin) message;
            // only mid-traversal V()/E() traversers can be non-locally processed
            assert !(traverser.get() instanceof Element) ||
                    this.worker.partition().contains((Element) traverser.get()) ||
                    this.matrix.getStepById(traverser.getStepId()) instanceof GraphStep;
            // attach traverser to the worker's partition
            TraversalActorProgram.attach(traverser, this.worker.partition());
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            if (step instanceof Barrier) {
                this.barriers.put(step.getId(), (Barrier) step);
            } else {
                while (step.hasNext()) {
                    this.sendTraverser(step.next());
                }
            }
        } else if (message instanceof SideEffectSetMessage) {
            ////////// UPDATE LOCAL SIDE-EFFECTS //////////
            this.matrix.getTraversal().getSideEffects().set(
                    ((SideEffectSetMessage) message).getKey(),
                    TraversalActorProgram.attach(((SideEffectSetMessage) message).getValue(), this.worker.partition()));
        } else if (message instanceof BarrierDoneMessage) {
            ////////// FINALIZE BARRIER SYNCHRONIZATION //////////
            final Step<?, ?> step = (Step) this.matrix.getStepById(((BarrierDoneMessage) message).getStepId());
            if (step instanceof LocalBarrier) { // the worker drains the local barrier
                while (step.hasNext()) {
                    sendTraverser(step.next());
                }
            } else
                ((Barrier) step).done();       // the master drains the global barrier
        } else if (message instanceof Boolean) {
            ////////// DETERMINE TERMINATION CONDITION //////////
            if (this.voteToHalt && !this.barriers.isEmpty()) {
                for (final Barrier barrier : this.barriers.values()) {
                    if (barrier instanceof LocalBarrier) {
                        barrier.processAllStarts();
                        this.worker.send(this.worker.master(), new BarrierAddMessage(barrier));
                    } else {
                        while (barrier.hasNextBarrier()) {
                            this.worker.send(this.worker.master(), new BarrierAddMessage(barrier));
                        }
                    }
                }
                this.barriers.clear();
                this.voteToHalt = false;
            }
            // use termination token to determine termination condition
            this.worker.send(this.neighborAddress, this.voteToHalt ? message : Boolean.FALSE);
            this.voteToHalt = true;
        } else {
            throw new IllegalArgumentException("The following message is unknown: " + message);
        }
    }

    @Override
    public void terminate() {

    }

    //////////////

    private void sendTraverser(final Traverser.Admin traverser) {
        this.voteToHalt = false;
        this.detachTraverser(traverser);
        if (traverser.isHalted()) {
            // send halted traversers to master
            this.worker.send(this.worker.master(), traverser);
        } else if (this.matrix.getStepById(traverser.getStepId()) instanceof GraphStep) {
            // mid-traversal V()/E() traversers need to be broadcasted across all workers/partitions
            this.worker.broadcast(traverser);
        } else if (traverser.get() instanceof Element && !this.worker.partition().contains((Element) traverser.get())) {
            // if the traverser references a non-local element, send the traverser to the appropriate worker/partition
            this.worker.send(this.partitionToWorkerMap.get(this.worker.partition().partitioner().find((Element) traverser.get())), traverser);
        } else {
            // if the traverser is local to the worker, send traverser to self
            this.worker.send(this.worker.address(), traverser);
        }
    }

    private final Traverser.Admin detachTraverser(final Traverser.Admin traverser) {
        return TraversalActorProgram.DETACH ? traverser.detach() : traverser;
    }
}
