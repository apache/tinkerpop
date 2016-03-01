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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.StepPosition;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * TraversalVertexProgram enables the evaluation of a {@link Traversal} on a {@link org.apache.tinkerpop.gremlin.process.computer.GraphComputer}.
 * At the start of the computation, each {@link Vertex} (or {@link org.apache.tinkerpop.gremlin.structure.Edge}) is assigned a single {@link Traverser}.
 * For each traverser that is local to the vertex, the vertex looks up its current location in the traversal and processes that step.
 * If the outputted traverser of the step references a local structure on the vertex (e.g. the vertex, an incident edge, its properties, or an arbitrary object),
 * then the vertex continues to compute the next traverser. If the traverser references another location in the graph,
 * then the traverser is sent to that location in the graph via a message. The messages of TraversalVertexProgram are traversers.
 * This continues until all traversers in the computation have halted.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgram implements VertexProgram<TraverserSet<Object>> {

    public static final String TRAVERSAL = "gremlin.traversalVertexProgram.traversal";
    public static final String HALTED_TRAVERSERS = "gremlin.traversalVertexProgram.haltedTraversers";
    public static final String ACTIVE_TRAVERSERS = "gremlin.traversalVertexProgram.activeTraversers";
    protected static final String MUTATED_MEMORY_KEYS = "gremlin.traversalVertexProgram.mutatedMemoryKeys";
    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    private static final String COMPLETED_BARRIERS = "gremlin.traversalVertexProgram.completedBarriers";

    // TODO: if not an adjacent traversal, use Local message scope -- a dual messaging system.
    private static final Set<MessageScope> MESSAGE_SCOPES = new HashSet<>(Collections.singletonList(MessageScope.Global.instance()));
    private static final Set<String> PROGRAM_KEYS = new HashSet<>(Arrays.asList(HALTED_TRAVERSERS, ACTIVE_TRAVERSERS, MUTATED_MEMORY_KEYS, COMPLETED_BARRIERS, VOTE_TO_HALT));
    private Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();
    private Set<String> sideEffectKeys = new HashSet<>();
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(HALTED_TRAVERSERS, false),
            VertexComputeKey.of(TraversalSideEffects.SIDE_EFFECTS, false))); // TODO: when MapReducers are not longer, remove this

    private PureTraversal<?, ?> traversal;
    private TraversalMatrix<?, ?> traversalMatrix;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private boolean keepDistributedHaltedTraversers = true;

    private TraversalVertexProgram() {
    }

    /**
     * A helper method to yield a {@link Traversal} from the {@link Graph} and provided {@link Configuration}.
     *
     * @param graph         the graph that the traversal will run against
     * @param configuration The configuration containing the TRAVERSAL key.
     * @return the traversal supplied by the configuration
     */
    public static Traversal.Admin<?, ?> getTraversal(final Graph graph, final Configuration configuration) {
        return VertexProgram.<TraversalVertexProgram>createVertexProgram(graph, configuration).traversal.get();
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (!configuration.containsKey(TRAVERSAL))
            throw new IllegalArgumentException("The configuration does not have a traversal: " + TRAVERSAL);
        this.traversal = PureTraversal.loadState(configuration, TRAVERSAL, graph);
        if (!this.traversal.get().isLocked())
            this.traversal.get().applyStrategies();
        /// traversal is compiled and ready to be introspected
        this.traversalMatrix = new TraversalMatrix<>(this.traversal.get());
        // if results will be serialized out, don't save halted traversers across the cluster
        this.keepDistributedHaltedTraversers = !(this.traversal.get().getParent().asStep().getNextStep() instanceof ComputerResultStep);
        // register MapReducer memory compute keys
        this.memoryComputeKeys.add(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal.get())) {
            this.mapReducers.add(mapReducer.getMapReduce());
            this.memoryComputeKeys.add(MemoryComputeKey.of(mapReducer.getMapReduce().getMemoryKey(), Operator.assign, false, false));
        }
        // register memory computing steps that use memory compute keys
        for (final MemoryComputing<?> memoryComputing : TraversalHelper.getStepsOfAssignableClassRecursively(MemoryComputing.class, this.traversal.get())) {
            this.memoryComputeKeys.add(memoryComputing.getMemoryComputeKey());
            this.sideEffectKeys.add(memoryComputing.getMemoryComputeKey().getKey()); // TODO: when no more MapReducers, you can remove this
        }
        // register TraversalVertexProgram specific memory compute keys
        this.memoryComputeKeys.add(MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false));
        this.memoryComputeKeys.add(MemoryComputeKey.of(ACTIVE_TRAVERSERS, Operator.addAll, true, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(MUTATED_MEMORY_KEYS, Operator.addAll, false, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(COMPLETED_BARRIERS, Operator.addAll, true, true));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.traversal.storeState(configuration, TRAVERSAL);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
        memory.set(HALTED_TRAVERSERS, new TraverserSet<>());
        memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
        memory.set(COMPLETED_BARRIERS, new HashSet<>());
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<Object>> messenger, final Memory memory) {
        this.traversal.get().getSideEffects().setLocalVertex(vertex);
        // if a barrier was completed in another worker, it is also completed here
        final Set<String> completedBarriers = memory.get(COMPLETED_BARRIERS);
        for (final String stepId : completedBarriers) {
            ((Barrier) this.traversalMatrix.getStepById(stepId)).done();
        }
        if (memory.isInitialIteration()) {    // ITERATION 1
            final TraverserSet<Object> haltedTraversers = vertex.<TraverserSet<Object>>property(HALTED_TRAVERSERS).orElse(new TraverserSet<>());
            vertex.property(VertexProperty.Cardinality.single, HALTED_TRAVERSERS, haltedTraversers);
            final TraverserSet<Object> activeTraversers = new TraverserSet<>();
            IteratorUtils.removeOnNext(haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.get().getStartStep().getId());
                activeTraversers.add(traverser);
            });
            assert haltedTraversers.isEmpty();
            if (this.traversal.get().getStartStep() instanceof GraphStep) {
                final GraphStep<Element, Element> graphStep = (GraphStep<Element, Element>) this.traversal.get().getStartStep();
                graphStep.reset();
                activeTraversers.forEach(traverser -> graphStep.addStart((Traverser.Admin) traverser));
                activeTraversers.clear();
                if (graphStep.returnsVertex())
                    graphStep.setIteratorSupplier(() -> ElementHelper.idExists(vertex.id(), graphStep.getIds()) ? (Iterator) IteratorUtils.of(vertex) : EmptyIterator.instance());
                else
                    graphStep.setIteratorSupplier(() -> (Iterator) IteratorUtils.filter(vertex.edges(Direction.OUT), edge -> ElementHelper.idExists(edge.id(), graphStep.getIds())));
                graphStep.forEachRemaining(traverser -> {
                    if (traverser.asAdmin().isHalted()) {
                        traverser.asAdmin().detach();
                        haltedTraversers.add((Traverser.Admin) traverser);
                        memory.add(HALTED_TRAVERSERS, new TraverserSet<>(traverser.asAdmin()));
                    } else
                        activeTraversers.add((Traverser.Admin) traverser);
                });
            }
            memory.add(VOTE_TO_HALT, activeTraversers.isEmpty() || TraverserExecutor.execute(vertex, new SingleMessenger<>(messenger, activeTraversers), this.traversalMatrix, memory));
        } else {  // ITERATION 1+
            memory.add(VOTE_TO_HALT, TraverserExecutor.execute(vertex, messenger, this.traversalMatrix, memory));
        }
        this.traversal.get().getSideEffects().forEach((key, value) -> {
            if (this.sideEffectKeys.contains(key) && vertex.<Map<String, Object>>property(VertexTraversalSideEffects.SIDE_EFFECTS).value().containsKey(key)) {
                memory.add(key, value);
                vertex.<Map<String, Object>>property(VertexTraversalSideEffects.SIDE_EFFECTS).value().remove(key);
            }
        });
        if (!this.keepDistributedHaltedTraversers)
            vertex.<TraverserSet>property(HALTED_TRAVERSERS).value().clear();
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        memory.set(VOTE_TO_HALT, true);
        memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        // put all side-effect memory into traversal side-effects
        if (voteToHalt) {
            final Set<String> mutatedMemoryKeys = memory.get(MUTATED_MEMORY_KEYS);
            memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
            // put all memory side-effects into the traversal side-effects
            memory.keys().stream().
                    filter(key -> !PROGRAM_KEYS.contains(key)).
                    filter(key -> !StepPosition.isStepId(key)).
                    forEach(key -> this.traversal.get().getSideEffects().set(key, memory.get(key)));
            // local traverser sets to process
            TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            TraverserSet<Object> localActiveTraversers = new TraverserSet<>();
            final TraverserSet<Object> remoteActiveTraversers = new TraverserSet<>();
            final TraverserSet<Object> haltedTraversers = memory.get(HALTED_TRAVERSERS);
            // get all barrier traversers
            final Set<String> completedBarriers = new HashSet<>();
            this.processMemory(memory, mutatedMemoryKeys, toProcessTraversers, completedBarriers);
            // process all results from barriers locally and when elements are touched, put them in remoteActiveTraversers
            while (!toProcessTraversers.isEmpty()) {
                this.processTraversers(toProcessTraversers, localActiveTraversers, remoteActiveTraversers, haltedTraversers);
                toProcessTraversers = localActiveTraversers;
                localActiveTraversers = new TraverserSet<>();
            }
            // put all traversal side-effects into memory
            this.traversal.get().getSideEffects().keys().forEach(key -> {
                if (memory.exists(key))
                    memory.set(key, this.traversal.get().getSideEffects().get(key).get());
            });
            // tell parallel barriers that might not have been active in the last round that they are no longer active
            memory.set(COMPLETED_BARRIERS, completedBarriers);
            if (!remoteActiveTraversers.isEmpty()) {
                // send active traversers back to workers
                memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
                return false;
            } else {
                // finalize locally with any last traversers dangling in the local traversal
                while (this.traversal.get().getEndStep().hasNext()) {
                    final Traverser.Admin<Object> traverser = (Traverser.Admin) this.traversal.get().getEndStep().next().asAdmin();
                    traverser.detach();
                    haltedTraversers.add(traverser);
                }
                // the result of a TraversalVertexProgram are the halted traversers
                memory.set(HALTED_TRAVERSERS, haltedTraversers);
                return true;
            }
        } else {
            return false;
        }
    }

    private void processMemory(final Memory memory, final Set<String> toProcessMemoryKeys, final TraverserSet<Object> traverserSet, final Set<String> completedBarriers) {
        for (final String key : toProcessMemoryKeys) {
            final Step<Object, Object> step = this.traversalMatrix.getStepById(key);
            if (null == step) continue;
            assert step instanceof Barrier;
            final Barrier<Object> barrier = (Barrier<Object>) step;
            barrier.addBarrier(memory.get(key));
            while (step.hasNext()) {
                traverserSet.add(step.next().asAdmin());
            }
            completedBarriers.add(step.getId());
            if (step instanceof ReducingBarrierStep)
                memory.set(step.getId(), ((ReducingBarrierStep) step).getSeedSupplier().get());
        }
    }

    private void processTraversers(final TraverserSet<Object> toProcessTraversers, final TraverserSet<Object> localActiveTraversers, final TraverserSet<Object> remoteActiveTraversers, final TraverserSet<Object> haltedTraversers) {
        Step<Object, Object> previousStep = EmptyStep.instance();
        Step<Object, Object> currentStep = EmptyStep.instance();

        final Iterator<Traverser.Admin<Object>> traversers = IteratorUtils.removeOnNext(toProcessTraversers.iterator());
        while (traversers.hasNext()) {
            final Traverser.Admin<Object> traverser = traversers.next();
            traverser.set(DetachedFactory.detach(traverser.get(), true));
            traverser.setSideEffects(this.traversal.get().getSideEffects());
            if (traverser.isHalted()) {
                traverser.asAdmin().detach();
                haltedTraversers.add(traverser);
            } else if (traverser.get() instanceof Attachable &&
                    !(traverser.get() instanceof Path) &&
                    !TraversalHelper.isLocalElement(this.traversalMatrix.getStepById(traverser.getStepId()))) {  // this is so that patterns like order().name work as expected.
                remoteActiveTraversers.add(traverser);
            } else {
                currentStep = this.traversalMatrix.getStepById(traverser.getStepId());
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep)) {
                    previousStep.forEachRemaining(result -> {
                        if (result.asAdmin().isHalted()) {
                            result.asAdmin().detach();
                            haltedTraversers.add(result.asAdmin());
                        } else {
                            if (result.get() instanceof Attachable)
                                remoteActiveTraversers.add(result.asAdmin());
                            else
                                localActiveTraversers.add(result.asAdmin());
                        }
                    });
                }
                currentStep.addStart(traverser);
                previousStep = currentStep;
            }
        }
        if (!(currentStep instanceof EmptyStep)) {
            currentStep.forEachRemaining(traverser -> {
                if (traverser.asAdmin().isHalted()) {
                    traverser.asAdmin().detach();
                    haltedTraversers.add(traverser.asAdmin());
                } else {
                    if (traverser.get() instanceof Attachable)
                        remoteActiveTraversers.add(traverser.asAdmin());
                    else
                        localActiveTraversers.add(traverser.asAdmin());
                }
            });
        }
        assert toProcessTraversers.isEmpty();
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public Set<MapReduce> getMapReducers() {
        return this.mapReducers;
    }

    @Override
    public Optional<MessageCombiner<TraverserSet<Object>>> getMessageCombiner() {
        return (Optional) TraversalVertexProgramMessageCombiner.instance();
    }

    @Override
    public TraversalVertexProgram clone() {
        try {
            final TraversalVertexProgram clone = (TraversalVertexProgram) super.clone();
            clone.traversal = this.traversal.clone();
            if (!clone.traversal.get().isLocked())
                clone.traversal.get().applyStrategies();
            clone.traversalMatrix = new TraversalMatrix<>(clone.traversal.get());
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public String toString() {
        final String traversalString = this.traversal.get().toString().substring(1);
        return StringFactory.vertexProgramString(this, traversalString.substring(0, traversalString.length() - 1));
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    //////////////

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(TraversalVertexProgram.class);
        }

        public Builder traversal(final TraversalSource traversalSource, final String scriptEngine, final String traversalScript, final Object... bindings) {
            return this.traversal(new ScriptTraversal<>(traversalSource, scriptEngine, traversalScript, bindings));
        }

        public Builder traversal(Traversal.Admin<?, ?> traversal) {
            // this is necessary if the job was submitted via TraversalVertexProgram.build() instead of TraversalVertexProgramStep.
            if (!(traversal.getParent() instanceof TraversalVertexProgramStep)) {
                final Traversal.Admin<?, ?> parentTraversal = new DefaultTraversal<>();
                traversal.getGraph().ifPresent(parentTraversal::setGraph);
                parentTraversal.setStrategies(traversal.getStrategies());
                parentTraversal.getStrategies().addStrategies(ComputerVerificationStrategy.instance(), new VertexProgramStrategy(Graph::compute));
                parentTraversal.addStep(new TraversalVertexProgramStep(parentTraversal, traversal));
                traversal = ((TraversalVertexProgramStep) parentTraversal.getStartStep()).getGlobalChildren().get(0);
            }
            PureTraversal.storeState(this.configuration, TRAVERSAL, traversal);
            return this;
        }
    }

}