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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final TraversalMatrix<?, ?> traversalMatrix, final Memory memory) {

        final TraversalSideEffects traversalSideEffects = traversalMatrix.getTraversal().getSideEffects();
        final TraverserSet<Object> haltedTraversers = vertex.value(TraversalVertexProgram.HALTED_TRAVERSERS);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final TraverserSet<Object> activeTraversers = new TraverserSet<>();
        final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();

        final TraverserSet<Object> maybeAliveTraversers = memory.get(TraversalVertexProgram.ACTIVE_TRAVERSERS);
        final Iterator<Traverser.Admin<Object>> iterator = maybeAliveTraversers.iterator();
        while (iterator.hasNext()) {
            final Traverser.Admin<Object> traverser = iterator.next();
            if (vertex.equals(TraverserExecutor.getHostingVertex(traverser.get()))) {
                // iterator.remove(); ConcurrentModificationException
                traverser.setSideEffects(traversalSideEffects);
                traverser.attach(Attachable.Method.get(vertex));
                toProcessTraversers.add(traverser);
            }
        }

        final Iterator<TraverserSet<?>> messages = messenger.receiveMessages();
        while (messages.hasNext()) {
            final Iterator<Traverser.Admin<Object>> traversers = (Iterator) messages.next().iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                traverser.setSideEffects(traversalSideEffects);
                traverser.attach(Attachable.Method.get(vertex));
                toProcessTraversers.add((Traverser.Admin) traverser);
            }
        }
        // while there are still local traversers, process them until they leave the vertex or halt (i.e. isHalted()).
        while (!toProcessTraversers.isEmpty()) {
            // process local traversers and if alive, repeat, else halt.
            Step<?, ?> previousStep = EmptyStep.instance();
            Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                final Step<?, ?> currentStep = traversalMatrix.getStepById(traverser.getStepId());
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep))
                    TraverserExecutor.drainStep(previousStep, activeTraversers, haltedTraversers, memory);
                currentStep.addStart((Traverser.Admin) traverser);
                previousStep = currentStep;
            }
            TraverserExecutor.drainStep(previousStep, activeTraversers, haltedTraversers, memory);
            assert toProcessTraversers.isEmpty();
            // process all the local objects and send messages or store locally again
            if (!activeTraversers.isEmpty()) {
                traversers = activeTraversers.iterator();
                while (traversers.hasNext()) {
                    final Traverser.Admin<Object> traverser = traversers.next();
                    traversers.remove();
                    if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                        // if the element is remote, then message, else store it locally for re-processing
                        final Vertex hostingVertex = TraverserExecutor.getHostingVertex(traverser.get());
                        if (!vertex.equals(hostingVertex)) { // necessary for path access
                            voteToHalt.set(false);
                            traverser.detach();
                            messenger.sendMessage(MessageScope.Global.of(hostingVertex), new TraverserSet<>(traverser));
                        } else {
                            if (traverser.get() instanceof Attachable)   // necessary for path access to local object
                                traverser.attach(Attachable.Method.get(vertex));
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

    private static void drainStep(final Step<?, ?> step, final TraverserSet<?> activeTraversers, final TraverserSet<?> haltedTraversers, final Memory memory) {
        if (step instanceof Barrier) {
            if (step instanceof Bypassing)
                ((Bypassing) step).setBypass(true);
            final Barrier barrier = (Barrier) step;
            while (barrier.hasNextBarrier()) {
                memory.add(step.getId(), barrier.nextBarrier());
                memory.add(TraversalVertexProgram.MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
            }
        } else { // LOCAL PROCESSING
            step.forEachRemaining(traverser -> {
                if (traverser.asAdmin().isHalted()) {
                    traverser.asAdmin().detach();
                    haltedTraversers.add((Traverser.Admin) traverser);
                    memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(traverser.asAdmin().split()));
                } else {
                    activeTraversers.add((Traverser.Admin) traverser);
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