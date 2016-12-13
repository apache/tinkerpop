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
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.StartMessage;
import org.apache.tinkerpop.gremlin.akka.process.actor.message.VoteToHaltMessage;
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
public final class WorkerTraversalActor extends AbstractActor implements RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    // terminate token is passed around worker ring to gather termination consensus (dual-ring termination algorithm)
    public enum Terminate {
        MAYBE, YES, NO
    }

    private final TraversalMatrix<?, ?> matrix;
    private final Partition localPartition;
    private final Partitioner partitioner;
    //
    private final Map<String, ActorSelection> workers = new HashMap<>();
    private final String neighborWorker;
    private boolean isLeader;
    private Terminate terminate = null;
    private boolean voteToHalt = false;
    private Map<String, Barrier> barriers = new HashMap<>();

    public WorkerTraversalActor(final Traversal.Admin<?, ?> traversal, final Partition localPartition, final Partitioner partitioner) {
        System.out.println("worker[created]: " + self().path());
        // set up partition and traversal information
        this.localPartition = localPartition;
        this.partitioner = partitioner;
        final WorkerTraversalSideEffects sideEffects = new WorkerTraversalSideEffects(traversal.getSideEffects(), context());
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
        // create termination ring topology
        final int i = this.partitioner.getPartitions().indexOf(this.localPartition);
        this.neighborWorker = "../worker-" + this.partitioner.getPartitions().get(i == this.partitioner.getPartitions().size() - 1 ? 0 : i + 1).hashCode();
        this.isLeader = i == 0;

        receive(ReceiveBuilder.
                match(StartMessage.class, start -> {
                    // initial message from master that says: "start processing"
                    final GraphStep step = (GraphStep) this.matrix.getTraversal().getStartStep();
                    while (step.hasNext()) {
                        this.sendTraverser(step.next());
                    }
                    // internal vote to have in mailbox as final message to process
                    assert null == this.terminate;
                    if (this.isLeader) {
                        this.terminate = Terminate.MAYBE;
                        self().tell(VoteToHaltMessage.instance(), self());
                    }
                }).
                match(Traverser.Admin.class, traverser -> {
                    this.processTraverser(traverser);
                }).
                match(SideEffectAddMessage.class, sideEffect -> {
                    this.matrix.getTraversal().getSideEffects().set(sideEffect.getSideEffectKey(), sideEffect.getSideEffectValue());
                }).
                match(Terminate.class, terminate -> {
                    assert this.isLeader || this.terminate != Terminate.MAYBE;
                    this.terminate = terminate;
                    self().tell(VoteToHaltMessage.instance(), self());
                }).
                match(BarrierDoneMessage.class, barrierDone -> {
                    final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(barrierDone.getStepId());
                    while (step.hasNext()) {
                        sendTraverser(step.next());
                    }
                }).
                match(VoteToHaltMessage.class, haltSync -> {
                    // if there is a barrier and thus, halting at barrier, then process barrier
                    if (!this.barriers.isEmpty()) {
                        for (final Barrier barrier : this.barriers.values()) {
                            while (barrier.hasNextBarrier()) {
                                master().tell(new BarrierAddMessage(barrier), self());
                            }
                        }
                        this.barriers.clear();
                        this.voteToHalt = false;
                    }
                    // use termination token to determine termination condition
                    if (null != this.terminate) {
                        if (this.isLeader) {
                            if (this.voteToHalt && Terminate.YES == this.terminate)
                                master().tell(VoteToHaltMessage.instance(), self());
                            else
                                worker(this.neighborWorker).tell(Terminate.YES, self());
                        } else
                            worker(this.neighborWorker).tell(this.voteToHalt ? this.terminate : Terminate.NO, self());
                        this.terminate = null;
                        this.voteToHalt = true;
                    }
                }).build()
        );
    }

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
            master().tell(traverser, self());
        else if (traverser.get() instanceof Element && !this.localPartition.contains((Element) traverser.get()))
            worker("../worker-" + this.partitioner.getPartition((Element) traverser.get()).hashCode()).tell(traverser, self());
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

    private ActorRef master() {
        return context().parent();
    }
}

