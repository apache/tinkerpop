/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.traversal;

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.VertexTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class WorkerExecutor {

    private WorkerExecutor() {

    }

    protected static boolean execute(final Vertex vertex,
                                     final Messenger<TraverserSet<Object>> messenger,
                                     final TraversalMatrix<?, ?> traversalMatrix,
                                     final Memory memory,
                                     final boolean returnHaltedTraversers,
                                     final TraverserSet<Object> haltedTraversers,
                                     final HaltedTraverserStrategy haltedTraverserStrategy) {
        final TraversalSideEffects traversalSideEffects = traversalMatrix.getTraversal().getSideEffects();
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final TraverserSet<Object> activeTraversers = new TraverserSet<>();
        final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();

        ////////////////////////////////
        // GENERATE LOCAL TRAVERSERS //
        ///////////////////////////////

        // MASTER ACTIVE
        // these are traversers that are going from OLTP (master) to OLAP (workers)
        // these traversers were broadcasted from the master traversal to the workers for attachment
        final VertexTraverserSet<Object> maybeActiveTraversers = memory.get(TraversalVertexProgram.ACTIVE_TRAVERSERS);
        // some memory systems are interacted with by multiple threads and thus, concurrent modification can happen at iterator.remove().
        // its better to reduce the memory footprint and shorten the active traverser list so synchronization is worth it.
        // most distributed OLAP systems have the memory partitioned and thus, this synchronization does nothing.
        synchronized (maybeActiveTraversers) {
            if (!maybeActiveTraversers.isEmpty()) {
                final Collection<Traverser.Admin<Object>> traversers = maybeActiveTraversers.get(vertex);
                if (traversers != null) {
                    final Iterator<Traverser.Admin<Object>> iterator = traversers.iterator();
                    while (iterator.hasNext()) {
                        final Traverser.Admin<Object> traverser = iterator.next();
                        iterator.remove();
                        maybeActiveTraversers.remove(traverser);
                        traverser.attach(Attachable.Method.get(vertex));
                        traverser.setSideEffects(traversalSideEffects);
                        toProcessTraversers.add(traverser);
                    }
                }
            }
        }

        // WORKER ACTIVE
        // these are traversers that exist from from a local barrier
        // these traversers will simply saved at the local vertex while the master traversal synchronized the barrier
        vertex.<TraverserSet<Object>>property(TraversalVertexProgram.ACTIVE_TRAVERSERS).ifPresent(previousActiveTraversers -> {
            IteratorUtils.removeOnNext(previousActiveTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.attach(Attachable.Method.get(vertex));
                traverser.setSideEffects(traversalSideEffects);
                toProcessTraversers.add(traverser);
            });
            assert previousActiveTraversers.isEmpty();
            // remove the property to save space
            vertex.property(TraversalVertexProgram.ACTIVE_TRAVERSERS).remove();
        });

        // TRAVERSER MESSAGES (WORKER -> WORKER)
        // these are traversers that have been messaged to the vertex from another vertex
        final Iterator<TraverserSet<Object>> messages = messenger.receiveMessages();
        while (messages.hasNext()) {
            IteratorUtils.removeOnNext(messages.next().iterator()).forEachRemaining(traverser -> {
                if (traverser.isHalted()) {
                    if (returnHaltedTraversers)
                        memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(haltedTraverserStrategy.halt(traverser)));
                    else
                        haltedTraversers.add(traverser); // the traverser has already been detached so no need to detach it again
                } else {
                    // traverser is not halted and thus, should be processed locally
                    // attach it and process
                    traverser.attach(Attachable.Method.get(vertex));
                    traverser.setSideEffects(traversalSideEffects);
                    toProcessTraversers.add(traverser);
                }
            });
        }

        ///////////////////////////////
        // PROCESS LOCAL TRAVERSERS //
        //////////////////////////////

        // while there are still local traversers, process them until they leave the vertex (message pass) or halt (store).
        while (!toProcessTraversers.isEmpty()) {
            Step<Object, Object> previousStep = EmptyStep.instance();
            Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                final Step<Object, Object> currentStep = traversalMatrix.getStepById(traverser.getStepId());
                // try and fill up the current step as much as possible with traversers to get a bulking optimization
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep))
                    WorkerExecutor.drainStep(vertex, previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers, haltedTraverserStrategy);
                currentStep.addStart(traverser);
                previousStep = currentStep;
            }
            WorkerExecutor.drainStep(vertex, previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers, haltedTraverserStrategy);
            // all processed traversers should be either halted or active
            assert toProcessTraversers.isEmpty();
            // process all the local objects and send messages or store locally again
            if (!activeTraversers.isEmpty()) {
                traversers = activeTraversers.iterator();
                while (traversers.hasNext()) {
                    final Traverser.Admin<Object> traverser = traversers.next();
                    traversers.remove();
                    // decide whether to message the traverser or to process it locally
                    if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                        // if the element is remote, then message, else store it locally for re-processing
                        final Vertex hostingVertex = WorkerExecutor.getHostingVertex(traverser.get());
                        if (!vertex.equals(hostingVertex)) { // if its host is not the current vertex, then send the traverser to the hosting vertex
                            voteToHalt.set(false); // if message is passed, then don't vote to halt
                            messenger.sendMessage(MessageScope.Global.of(hostingVertex), new TraverserSet<>(traverser.detach()));
                        } else {
                            traverser.attach(Attachable.Method.get(vertex)); // necessary for select() steps that reference the current object
                            toProcessTraversers.add(traverser);
                        }
                    } else                                                                              // STANDARD OBJECT
                        toProcessTraversers.add(traverser);
                }
                assert activeTraversers.isEmpty();
            }
        }
        return voteToHalt.get();
    }

    private static void drainStep(final Vertex vertex,
                                  final Step<Object, Object> step,
                                  final TraverserSet<Object> activeTraversers,
                                  final TraverserSet<Object> haltedTraversers,
                                  final Memory memory,
                                  final boolean returnHaltedTraversers,
                                  final HaltedTraverserStrategy haltedTraverserStrategy) {
        GraphComputing.atMaster(step, false);
        if (step instanceof Barrier) {
            if (step instanceof Bypassing)
                ((Bypassing) step).setBypass(true);
            if (step instanceof LocalBarrier) {
                // local barrier traversers are stored on the vertex until the master traversal synchronizes the system
                final LocalBarrier<Object> barrier = (LocalBarrier<Object>) step;
                final TraverserSet<Object> localBarrierTraversers = vertex.<TraverserSet<Object>>property(TraversalVertexProgram.ACTIVE_TRAVERSERS).orElse(new TraverserSet<>());
                vertex.property(TraversalVertexProgram.ACTIVE_TRAVERSERS, localBarrierTraversers);
                while (barrier.hasNextBarrier()) {
                    final TraverserSet<Object> barrierSet = barrier.nextBarrier();
                    IteratorUtils.removeOnNext(barrierSet.iterator()).forEachRemaining(traverser -> {
                        traverser.addLabels(step.getLabels());  // this might need to be generalized for working with global barriers too
                        if (traverser.isHalted() &&
                                (returnHaltedTraversers ||
                                        (!(traverser.get() instanceof Element) && !(traverser.get() instanceof Property)) ||
                                        getHostingVertex(traverser.get()).equals(vertex))) {
                            if (returnHaltedTraversers)
                                memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(haltedTraverserStrategy.halt(traverser)));
                            else
                                haltedTraversers.add(traverser.detach());
                        } else
                            localBarrierTraversers.add(traverser.detach());
                    });
                }
                memory.add(TraversalVertexProgram.MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
            } else {
                final Barrier barrier = (Barrier) step;
                while (barrier.hasNextBarrier()) {
                    memory.add(step.getId(), barrier.nextBarrier());
                }
                memory.add(TraversalVertexProgram.MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
            }
        } else { // LOCAL PROCESSING
            step.forEachRemaining(traverser -> {
                if (traverser.isHalted() &&
                        // if its a ReferenceFactory (one less iteration required)
                        ((returnHaltedTraversers || ReferenceFactory.class == haltedTraverserStrategy.getHaltedTraverserFactory()) &&
                                (!(traverser.get() instanceof Element) && !(traverser.get() instanceof Property)) ||
                                getHostingVertex(traverser.get()).equals(vertex))) {
                    if (returnHaltedTraversers)
                        memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(haltedTraverserStrategy.halt(traverser)));
                    else
                        haltedTraversers.add(traverser.detach());
                } else {
                    activeTraversers.add(traverser);
                }
            });
        }
    }

    private static Vertex getHostingVertex(final Object object) {
        Object obj = object;
        while (true) {
            if (obj instanceof Vertex)
                return (Vertex) obj;
            else if (obj instanceof Edge)
                return ((Edge) obj).outVertex();
            else if (obj instanceof Property)
                obj = ((Property) obj).element();
            else
                throw new IllegalStateException("The host of the object is unknown: " + obj.toString() + ':' + obj.getClass().getCanonicalName());
        }
    }
}