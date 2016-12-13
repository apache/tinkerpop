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
import org.apache.tinkerpop.gremlin.process.actor.Address;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.SideEffectSetMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.StartMessage;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.Terminate;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.VoteToHaltMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TraversalWorkerProgram<M> implements ActorProgram.Worker<M> {


    private final Actor.Worker self;
    private final TraversalMatrix<?, ?> matrix;
    private final Partition localPartition;
    private final Partitioner partitioner;
    //
    private Address.Worker neighborWorker;
    private boolean isLeader;
    private Terminate terminate = null;
    private boolean voteToHalt = false;
    private Map<String, Barrier> barriers = new HashMap<>();

    public TraversalWorkerProgram(final Actor.Worker self, final Traversal.Admin<?, ?> traversal, final Partitioner partitioner) {
        this.self = self;
        // System.out.println("worker[created]: " + this.self.address().location());
        // set up partition and traversal information
        this.partitioner = partitioner;
        this.localPartition = self.partition();
        final WorkerTraversalSideEffects sideEffects = new WorkerTraversalSideEffects(traversal.getSideEffects(), this.self);
        TraversalHelper.applyTraversalRecursively(t -> t.setSideEffects(sideEffects), traversal);
        this.matrix = new TraversalMatrix<>(traversal);
        final GraphStep graphStep = (GraphStep) traversal.getStartStep();
        if (0 == graphStep.getIds().length)
            ((GraphStep) traversal.getStartStep()).setIteratorSupplier(graphStep.returnsVertex() ? this.localPartition::vertices : this.localPartition::edges);
        else {
            if (graphStep.returnsVertex())
                ((GraphStep<Vertex, Vertex>) traversal.getStartStep()).setIteratorSupplier(
                        () -> IteratorUtils.filter(this.localPartition.vertices(graphStep.getIds()), this.localPartition::contains));
            else
                ((GraphStep<Edge, Edge>) traversal.getStartStep()).setIteratorSupplier(
                        () -> IteratorUtils.filter(this.localPartition.edges(graphStep.getIds()), this.localPartition::contains));
        }
    }

    @Override
    public void setup() {
        // create termination ring topology
        final int i = this.self.workers().indexOf(this.self.address());
        this.neighborWorker = this.self.workers().get(i == this.self.workers().size() - 1 ? 0 : i + 1);
        this.isLeader = i == 0;
    }

    @Override
    public void execute(final M message) {
        //System.out.println(message + "::" + this.isLeader);
        if (message instanceof StartMessage) {
            // initial message from master that says: "start processing"
            final GraphStep step = (GraphStep) this.matrix.getTraversal().getStartStep();
            while (step.hasNext()) {
                this.sendTraverser(step.next());
            }
            // internal vote to have in mailbox as final message to process
            // TODO: assert null == this.terminate;
            if (this.isLeader) {
                this.terminate = Terminate.MAYBE;
                this.self.send(this.self.address(), VoteToHaltMessage.instance());
            }
        } else if (message instanceof Traverser.Admin) {
            final Traverser.Admin<?> traverser = (Traverser.Admin) message;
            this.processTraverser(traverser);

        } else if (message instanceof SideEffectSetMessage) {
            this.matrix.getTraversal().getSideEffects().set(((SideEffectSetMessage) message).getKey(), ((SideEffectSetMessage) message).getValue());
        } else if (message instanceof Terminate) {
            assert this.isLeader || this.terminate != Terminate.MAYBE;
            this.terminate = (Terminate) message;
            this.self.send(this.self.address(), VoteToHaltMessage.instance());
        } else if (message instanceof VoteToHaltMessage) {
            // if there is a barrier and thus, halting at barrier, then process barrier
            if (!this.barriers.isEmpty()) {
                for (final Barrier barrier : this.barriers.values()) {
                    while (barrier.hasNextBarrier()) {
                        this.self.send(this.self.master(), new BarrierAddMessage(barrier));
                    }
                }
                this.barriers.clear();
                this.voteToHalt = false;
            }
            // use termination token to determine termination condition
            if (null != this.terminate) {
                if (this.isLeader) {
                    if (this.voteToHalt && Terminate.YES == this.terminate)
                        this.self.send(this.self.master(), VoteToHaltMessage.instance());
                    else
                        this.self.send(this.neighborWorker, Terminate.YES);
                } else
                    this.self.send(this.neighborWorker, this.voteToHalt ? this.terminate : Terminate.NO);
                this.terminate = null;
                this.voteToHalt = true;
            }
        } else if (message instanceof BarrierDoneMessage) {
            final Step<?, ?> step = (Step) this.matrix.getStepById(((BarrierDoneMessage) message).getStepId());
            while (step.hasNext()) {
                sendTraverser(step.next());
            }
        } else {
            throw new IllegalArgumentException("The following message is unknown: " + message);
        }
    }

    @Override
    public void terminate() {

    }

    //////////////

    private void processTraverser(final Traverser.Admin traverser) {
        assert !(traverser.get() instanceof Element) || !traverser.isHalted() || this.localPartition.contains((Element) traverser.get());
        final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
        if (step instanceof Bypassing) ((Bypassing) step).setBypass(true);
        GraphComputing.atMaster(step, false);
        step.addStart(traverser);
        if (step instanceof Barrier) {
            this.barriers.put(step.getId(), (Barrier) step);
        } else {
            while (step.hasNext()) {
                this.sendTraverser(step.next());
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        this.voteToHalt = false;
        if (traverser.isHalted())
            this.self.send(this.self.master(), traverser);
        else if (traverser.get() instanceof Element && !this.localPartition.contains((Element) traverser.get()))
            this.self.send(this.self.workers().get(this.partitioner.getPartitions().indexOf(this.partitioner.getPartition((Element) traverser.get()))), traverser);
        else
            this.self.send(this.self.address(), traverser);
    }
}
