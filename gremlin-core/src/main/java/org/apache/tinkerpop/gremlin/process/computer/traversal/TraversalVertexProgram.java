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
import org.apache.tinkerpop.gremlin.process.computer.Computer;
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
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
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
import org.apache.tinkerpop.gremlin.util.function.MutableMetricsSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
    private Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS =
            new HashSet<>(Arrays.asList(VertexComputeKey.of(HALTED_TRAVERSERS, false), VertexComputeKey.of(ACTIVE_TRAVERSERS, true)));

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

    /**
     * Get the {@link Traversal} associated with the current instance of the traversal vertex program.
     *
     * @return the traversal of the instantiated program
     */
    public Traversal.Admin<?, ?> getTraversal() {
        return this.traversal.get();
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
        this.keepDistributedHaltedTraversers =
                !(this.traversal.get().getParent().asStep().getNextStep() instanceof ComputerResultStep || // if its just going to stream it out, don't distribute
                        this.traversal.get().getParent().asStep().getNextStep() instanceof EmptyStep ||  // same as above, but if using TraversalVertexProgramStep directly
                        (this.traversal.get().getParent().asStep().getNextStep() instanceof ProfileStep && // same as above, but needed for profiling
                                this.traversal.get().getParent().asStep().getNextStep().getNextStep() instanceof ComputerResultStep));
        // register traversal side-effects in memory
        final TraversalSideEffects sideEffects = ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).getSideEffects();
        sideEffects.keys().forEach(key -> this.memoryComputeKeys.add(MemoryComputeKey.of(key, sideEffects.getReducer(key), true, false)));
        // register MapReducer memory compute keys
        this.memoryComputeKeys.add(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal.get())) {
            this.mapReducers.add(mapReducer.getMapReduce());
            this.memoryComputeKeys.add(MemoryComputeKey.of(mapReducer.getMapReduce().getMemoryKey(), Operator.assign, false, false));
        }
        // register memory computing steps that use memory compute keys
        for (final MemoryComputing<?> memoryComputing : TraversalHelper.getStepsOfAssignableClassRecursively(MemoryComputing.class, this.traversal.get())) {
            this.memoryComputeKeys.add(memoryComputing.getMemoryComputeKey());
        }
        // register profile steps (TODO: try to hide this)
        for (final ProfileStep profileStep : TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get())) {
            this.traversal.get().getSideEffects().register(profileStep.getId(), new MutableMetricsSupplier(profileStep.getPreviousStep()), ProfileStep.ProfileBiOperator.instance());
        }
        // register TraversalVertexProgram specific memory compute keys
        this.memoryComputeKeys.add(MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, this.keepDistributedHaltedTraversers)); // only keep if it will be preserved
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
        // memory is local
        ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).setMemory(memory, false);
        memory.set(VOTE_TO_HALT, true);
        memory.set(HALTED_TRAVERSERS, new TraverserSet<>());
        memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
        memory.set(COMPLETED_BARRIERS, new HashSet<>());
        final TraversalSideEffects sideEffects = ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).getSideEffects();
        sideEffects.keys().forEach(key -> memory.set(key, sideEffects.get(key)));
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<Object>> messenger, final Memory memory) {
        // memory is distributed
        ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).setMemory(memory, true);
        // if a barrier was completed in another worker, it is also completed here (ensure distributed barries are synchronized)
        final Set<String> completedBarriers = memory.get(COMPLETED_BARRIERS);
        for (final String stepId : completedBarriers) {
            final Step<?, ?> step = this.traversalMatrix.getStepById(stepId);
            if (step instanceof Barrier)
                ((Barrier) this.traversalMatrix.getStepById(stepId)).done();
        }
        //////////////////
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
                    if (traverser.isHalted()) {
                        traverser.detach();
                        haltedTraversers.add((Traverser.Admin) traverser);
                        if (!this.keepDistributedHaltedTraversers)
                            memory.add(HALTED_TRAVERSERS, new TraverserSet<>(traverser));
                    } else
                        activeTraversers.add((Traverser.Admin) traverser);
                });
            }
            memory.add(VOTE_TO_HALT, activeTraversers.isEmpty() || TraverserExecutor.execute(vertex, new SingleMessenger<>(messenger, activeTraversers), this.traversalMatrix, memory, !this.keepDistributedHaltedTraversers));
        } else {  // ITERATION 1+
            memory.add(VOTE_TO_HALT, TraverserExecutor.execute(vertex, messenger, this.traversalMatrix, memory, !this.keepDistributedHaltedTraversers));
        }
        if (!this.keepDistributedHaltedTraversers)
            vertex.<TraverserSet>property(HALTED_TRAVERSERS).value().clear();
    }

    @Override
    public boolean terminate(final Memory memory) {
        // memory is local
        ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).setMemory(memory, false);
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        memory.set(VOTE_TO_HALT, true);
        memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        if (voteToHalt) {
            final Set<String> mutatedMemoryKeys = memory.get(MUTATED_MEMORY_KEYS);
            memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
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
            // tell parallel barriers that might not have been active in the last round that they are no longer active
            memory.set(COMPLETED_BARRIERS, completedBarriers);
            if (!remoteActiveTraversers.isEmpty() ||
                    completedBarriers.stream().map(this.traversalMatrix::getStepById).filter(step -> step instanceof LocalBarrier).findAny().isPresent()) {
                // send active traversers back to workers
                memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
                return false;
            } else {
                // finalize locally with any last traversers dangling in the local traversal
                while (this.traversal.get().getEndStep().hasNext()) {
                    final Traverser.Admin traverser = this.traversal.get().getEndStep().next();
                    traverser.detach();
                    haltedTraversers.add(traverser);
                }
                // the result of a TraversalVertexProgram are the halted traversers
                memory.set(HALTED_TRAVERSERS, haltedTraversers);
                // finalize profile side-effect steps. (todo: try and hide this)
                for (final ProfileSideEffectStep profileStep : TraversalHelper.getStepsOfAssignableClassRecursively(ProfileSideEffectStep.class, this.traversal.get())) {
                    this.traversal.get().getSideEffects().set(profileStep.getSideEffectKey(), profileStep.generateFinalResult(this.traversal.get().getSideEffects().get(profileStep.getSideEffectKey())));
                }
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
            completedBarriers.add(step.getId());
            if (!(step instanceof LocalBarrier)) {  // local barriers don't do any processing on the master traversal (they just lock on the workers)
                final Barrier<Object> barrier = (Barrier<Object>) step;
                barrier.addBarrier(memory.get(key));
                while (step.hasNext()) {
                    traverserSet.add(step.next());
                }
                if (step instanceof ReducingBarrierStep)
                    memory.set(step.getId(), ((ReducingBarrierStep) step).getSeedSupplier().get());
            }
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
                traverser.detach();
                haltedTraversers.add(traverser);
            } else if (this.isRemoteTraverser(traverser)) {  // this is so that patterns like order().name work as expected.
                traverser.detach();
                remoteActiveTraversers.add(traverser);
            } else {
                currentStep = this.traversalMatrix.getStepById(traverser.getStepId());
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep)) {
                    previousStep.forEachRemaining(result -> {
                        if (result.isHalted()) {
                            result.detach();
                            haltedTraversers.add(result);
                        } else {
                            if (this.isRemoteTraverser(result)) {
                                result.detach();
                                remoteActiveTraversers.add(result);
                            } else
                                localActiveTraversers.add(result);
                        }
                    });
                }
                currentStep.addStart(traverser);
                previousStep = currentStep;
            }
        }
        if (!(currentStep instanceof EmptyStep)) {
            currentStep.forEachRemaining(traverser -> {
                if (traverser.isHalted()) {
                    traverser.detach();
                    haltedTraversers.add(traverser);
                } else {
                    if (this.isRemoteTraverser(traverser)) {
                        traverser.detach();
                        remoteActiveTraversers.add(traverser);
                    } else
                        localActiveTraversers.add(traverser);
                }
            });
        }
        assert toProcessTraversers.isEmpty();
    }

    private boolean isRemoteTraverser(final Traverser.Admin traverser) {
        return traverser.get() instanceof Attachable &&
                !(traverser.get() instanceof Path) &&
                !isLocalElement(this.traversalMatrix.getStepById(traverser.getStepId()));
    }

    // TODO: once this is complete (fully known), move to TraversalHelper
    private static boolean isLocalElement(final Step<?, ?> step) {
        return step instanceof PropertiesStep || step instanceof PropertyMapStep ||
                step instanceof IdStep || step instanceof LabelStep || step instanceof SackStep ||
                step instanceof PropertyKeyStep || step instanceof PropertyValueStep ||
                step instanceof TailGlobalStep || step instanceof RangeGlobalStep || step instanceof HasStep;
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
                final MemoryTraversalSideEffects memoryTraversalSideEffects = new MemoryTraversalSideEffects(traversal.getSideEffects());
                final Traversal.Admin<?, ?> parentTraversal = new DefaultTraversal<>();
                traversal.getGraph().ifPresent(parentTraversal::setGraph);
                final TraversalStrategies strategies = traversal.getStrategies().clone();
                strategies.addStrategies(ComputerVerificationStrategy.instance(), new VertexProgramStrategy(Computer.compute()));
                parentTraversal.setStrategies(strategies);
                traversal.setStrategies(strategies);
                parentTraversal.setSideEffects(memoryTraversalSideEffects);
                parentTraversal.addStep(new TraversalVertexProgramStep(parentTraversal, traversal));
                traversal = ((TraversalVertexProgramStep) parentTraversal.getStartStep()).getGlobalChildren().get(0);
                traversal.setSideEffects(memoryTraversalSideEffects);
            }
            PureTraversal.storeState(this.configuration, TRAVERSAL, traversal);
            return this;
        }
    }

}