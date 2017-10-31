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
import org.apache.tinkerpop.gremlin.process.computer.ProgramPhase;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.SingleMessenger;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.MutableMetricsSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
    private TraverserSet<Object> haltedTraversers;
    private boolean returnHaltedTraversers = false;
    private HaltedTraverserStrategy haltedTraverserStrategy;
    private boolean profile = false;
    // handle current profile metrics if profile is true
    private MutableMetrics iterationMetrics;


    private TraversalVertexProgram() {
    }

    /**
     * Get the {@link PureTraversal} associated with the current instance of the {@link TraversalVertexProgram}.
     *
     * @return the pure traversal of the instantiated program
     */
    public PureTraversal<?, ?> getTraversal() {
        return this.traversal;
    }

    public static <R> TraverserSet<R> loadHaltedTraversers(final Configuration configuration) {
        if (!configuration.containsKey(HALTED_TRAVERSERS))
            return new TraverserSet<>();

        final Object object = configuration.getProperty(HALTED_TRAVERSERS) instanceof String ?
                VertexProgramHelper.deserialize(configuration, HALTED_TRAVERSERS) :
                configuration.getProperty(HALTED_TRAVERSERS);
        if (object instanceof Traverser.Admin)
            return new TraverserSet<>((Traverser.Admin<R>) object);
        else {
            final TraverserSet<R> traverserSet = new TraverserSet<>();
            traverserSet.addAll((Collection) object);
            return traverserSet;
        }
    }

    public static <R> void storeHaltedTraversers(final Configuration configuration, final TraverserSet<R> haltedTraversers) {
        if (null != haltedTraversers && !haltedTraversers.isEmpty()) {
            try {
                VertexProgramHelper.serialize(haltedTraversers, configuration, HALTED_TRAVERSERS);
            } catch (final Exception e) {
                configuration.setProperty(HALTED_TRAVERSERS, haltedTraversers);
            }
        }
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
        // get any master-traversal halted traversers
        this.haltedTraversers = TraversalVertexProgram.loadHaltedTraversers(configuration);
        // if results will be serialized out, don't save halted traversers across the cluster
        this.returnHaltedTraversers =
                (this.traversal.get().getParent().asStep().getNextStep() instanceof ComputerResultStep || // if its just going to stream it out, don't distribute
                        this.traversal.get().getParent().asStep().getNextStep() instanceof EmptyStep ||  // same as above, but if using TraversalVertexProgramStep directly
                        (this.traversal.get().getParent().asStep().getNextStep() instanceof ProfileStep && // same as above, but needed for profiling
                                this.traversal.get().getParent().asStep().getNextStep().getNextStep() instanceof ComputerResultStep));
        // determine how to store halted traversers
        this.haltedTraverserStrategy = ((HaltedTraverserStrategy) this.traversal.get().getStrategies().toList()
                .stream()
                .filter(strategy -> strategy instanceof HaltedTraverserStrategy)
                .findAny()
                .orElse(HaltedTraverserStrategy.reference()));
        // register traversal side-effects in memory
        this.memoryComputeKeys.addAll(MemoryTraversalSideEffects.getMemoryComputeKeys(this.traversal.get()));
        // register MapReducer memory compute keys
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
        this.memoryComputeKeys.add(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false));
        this.memoryComputeKeys.add(MemoryComputeKey.of(ACTIVE_TRAVERSERS, Operator.addAll, true, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(MUTATED_MEMORY_KEYS, Operator.addAll, false, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(COMPLETED_BARRIERS, Operator.addAll, true, true));

        // does the traversal need profile information
        this.profile = !TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get()).isEmpty();
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.traversal.storeState(configuration, TRAVERSAL);
        TraversalVertexProgram.storeHaltedTraversers(configuration, this.haltedTraversers);
    }

    @Override
    public void setup(final Memory memory) {
        // memory is local
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.SETUP);
        ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).storeSideEffectsInMemory();
        memory.set(VOTE_TO_HALT, true);
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
        memory.set(COMPLETED_BARRIERS, new HashSet<>());
        // if halted traversers are being sent from a previous VertexProgram in an OLAP chain (non-distributed traversers), get them into the flow
        if (!this.haltedTraversers.isEmpty()) {
            final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            IteratorUtils.removeOnNext(this.haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.get().getStartStep().getId());
                toProcessTraversers.add(traverser);
            });
            assert this.haltedTraversers.isEmpty();
            final TraverserSet<Object> remoteActiveTraversers = new TraverserSet<>();
            MasterExecutor.processTraversers(this.traversal, this.traversalMatrix, toProcessTraversers, remoteActiveTraversers, this.haltedTraversers, this.haltedTraverserStrategy);
            memory.set(HALTED_TRAVERSERS, this.haltedTraversers);
            memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
        } else {
            memory.set(HALTED_TRAVERSERS, new TraverserSet<>());
            memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        }
        // local variable will no longer be used so null it for GC
        this.haltedTraversers = null;
        // does the traversal need profile information
        this.profile = !TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get()).isEmpty();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<Object>> messenger, final Memory memory) {
        // if any global halted traversers, simply don't use them as they were handled by master setup()
        // these halted traversers are typically from a previous OLAP job that yielded traversers at the master traversal
        if (null != this.haltedTraversers)
            this.haltedTraversers = null;
        // memory is distributed
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.EXECUTE);
        // if a barrier was completed in another worker, it is also completed here (ensure distributed barriers are synchronized)
        final Set<String> completedBarriers = memory.get(COMPLETED_BARRIERS);
        for (final String stepId : completedBarriers) {
            final Step<?, ?> step = this.traversalMatrix.getStepById(stepId);
            if (step instanceof Barrier)
                ((Barrier) this.traversalMatrix.getStepById(stepId)).done();
        }
        // define halted traversers
        final VertexProperty<TraverserSet<Object>> property = vertex.property(HALTED_TRAVERSERS);
        final TraverserSet<Object> haltedTraversers;
        if (property.isPresent()) {
            haltedTraversers = property.value();
        } else {
            haltedTraversers = new TraverserSet<>();
            vertex.property(VertexProperty.Cardinality.single, HALTED_TRAVERSERS, haltedTraversers);
        }
        //////////////////
        if (memory.isInitialIteration()) {    // ITERATION 1
            final TraverserSet<Object> activeTraversers = new TraverserSet<>();
            // if halted traversers are being sent from a previous VertexProgram in an OLAP chain (distributed traversers), get them into the flow
            IteratorUtils.removeOnNext(haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.get().getStartStep().getId());
                activeTraversers.add(traverser);
            });
            assert haltedTraversers.isEmpty();
            // for g.V()/E()
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
                        if (this.returnHaltedTraversers)
                            memory.add(HALTED_TRAVERSERS, new TraverserSet<>(this.haltedTraverserStrategy.halt(traverser)));
                        else
                            haltedTraversers.add((Traverser.Admin) traverser.detach());
                    } else
                        activeTraversers.add((Traverser.Admin) traverser);
                });
            }
            memory.add(VOTE_TO_HALT, activeTraversers.isEmpty() || WorkerExecutor.execute(vertex, new SingleMessenger<>(messenger, activeTraversers), this.traversalMatrix, memory, this.returnHaltedTraversers, haltedTraversers, this.haltedTraverserStrategy));
        } else   // ITERATION 1+
            memory.add(VOTE_TO_HALT, WorkerExecutor.execute(vertex, messenger, this.traversalMatrix, memory, this.returnHaltedTraversers, haltedTraversers, this.haltedTraverserStrategy));
        // save space by not having an empty halted traversers property
        if (this.returnHaltedTraversers || haltedTraversers.isEmpty())
            vertex.<TraverserSet>property(HALTED_TRAVERSERS).remove();
    }

    @Override
    public boolean terminate(final Memory memory) {
        // memory is local
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.TERMINATE);
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        memory.set(VOTE_TO_HALT, true);
        memory.set(ACTIVE_TRAVERSERS, new TraverserSet<>());
        if (voteToHalt) {
            // local traverser sets to process
            final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            // traversers that need to be sent back to the workers (no longer can be processed locally by the master traversal)
            final TraverserSet<Object> remoteActiveTraversers = new TraverserSet<>();
            // halted traversers that have completed their journey
            final TraverserSet<Object> haltedTraversers = memory.get(HALTED_TRAVERSERS);
            // get all barrier traversers
            final Set<String> completedBarriers = new HashSet<>();
            MasterExecutor.processMemory(this.traversalMatrix, memory, toProcessTraversers, completedBarriers);
            // process all results from barriers locally and when elements are touched, put them in remoteActiveTraversers
            MasterExecutor.processTraversers(this.traversal, this.traversalMatrix, toProcessTraversers, remoteActiveTraversers, haltedTraversers, this.haltedTraverserStrategy);
            // tell parallel barriers that might not have been active in the last round that they are no longer active
            memory.set(COMPLETED_BARRIERS, completedBarriers);
            if (!remoteActiveTraversers.isEmpty() ||
                    completedBarriers.stream().map(this.traversalMatrix::getStepById).filter(step -> step instanceof LocalBarrier).findAny().isPresent()) {
                // send active traversers back to workers
                memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
                return false;
            } else {
                // finalize locally with any last traversers dangling in the local traversal
                final Step<?, Object> endStep = (Step<?, Object>) this.traversal.get().getEndStep();
                while (endStep.hasNext()) {
                    haltedTraversers.add(this.haltedTraverserStrategy.halt(endStep.next()));
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

    @Override
    public void workerIterationStart(final Memory memory) {
        // start collecting profile metrics
        if (this.profile) {
            this.iterationMetrics = new MutableMetrics("iteration" + memory.getIteration(), "Worker Iteration " + memory.getIteration());
            this.iterationMetrics.start();
        }
    }

    @Override
    public void workerIterationEnd(final Memory memory) {
        // store profile metrics in proper ProfileStep metrics
        if (this.profile) {
            List<ProfileStep> profileSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get());
            // guess the profile step to store data
            int profileStepIndex = memory.getIteration();
            // if we guess wrongly write timing into last step
            profileStepIndex = profileStepIndex >= profileSteps.size() ? profileSteps.size() - 1 : profileStepIndex;
            this.iterationMetrics.finish(0);
            // reset counts
            this.iterationMetrics.setCount(TraversalMetrics.TRAVERSER_COUNT_ID,0);
            if (null != MemoryTraversalSideEffects.getMemorySideEffectsPhase(this.traversal.get())) {
                this.traversal.get().getSideEffects().add(profileSteps.get(profileStepIndex).getId(), this.iterationMetrics);
            }
            this.iterationMetrics = null;
        }
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
            clone.memoryComputeKeys = new HashSet<>();
            for (final MemoryComputeKey memoryComputeKey : this.memoryComputeKeys) {
                clone.memoryComputeKeys.add(memoryComputeKey.clone());
            }
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

        public Builder haltedTraversers(final TraverserSet<Object> haltedTraversers) {
            TraversalVertexProgram.storeHaltedTraversers(this.configuration, haltedTraversers);
            return this;
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
                strategies.addStrategies(ComputerFinalizationStrategy.instance(), ComputerVerificationStrategy.instance(), new VertexProgramStrategy(Computer.compute()));
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