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
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
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
public final class TraversalVertexProgram implements VertexProgram<TraverserSet<?>> {

    public static final String HALTED_TRAVERSERS = "gremlin.traversalVertexProgram.haltedTraversers";
    private static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    public static final String TRAVERSAL = "gremlin.traversalVertexProgram.traversal";

    // TODO: if not an adjacent traversal, use Local message scope -- a dual messaging system.
    private static final Set<MessageScope> MESSAGE_SCOPES = new HashSet<>(Collections.singletonList(MessageScope.Global.instance()));
    private Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();
    private Set<String> sideEffectKeys = new HashSet<>();
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(HALTED_TRAVERSERS, false),
            VertexComputeKey.of(TraversalSideEffects.SIDE_EFFECTS, false)));

    private PureTraversal<?, ?> traversal;
    private TraversalMatrix<?, ?> traversalMatrix;
    private final Set<MapReduce> mapReducers = new HashSet<>();

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
        this.traversalMatrix = new TraversalMatrix<>(this.traversal.get());
        this.memoryComputeKeys.add(MemoryComputeKey.of(VOTE_TO_HALT, MemoryComputeKey.andOperator(), false, true));
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal.get())) {
            this.mapReducers.add(mapReducer.getMapReduce());
            this.memoryComputeKeys.add(MemoryComputeKey.of(mapReducer.getMapReduce().getMemoryKey(), MemoryComputeKey.setOperator(), false, false));
        }
        if (!(this.traversal.get().getEndStep() instanceof SideEffectCapStep) && !(this.traversal.get().getEndStep() instanceof ReducingBarrierStep) && !(this.traversal.get().getEndStep() instanceof RangeGlobalStep)) {
            this.mapReducers.add(new TraverserMapReduce(this.traversal.get()));
        }

        for (final GraphComputing<?> graphComputing : TraversalHelper.getStepsOfAssignableClassRecursively(GraphComputing.class, this.traversal.get())) {
            graphComputing.getMemoryComputeKey().ifPresent(this.memoryComputeKeys::add);
            graphComputing.getMemoryComputeKey().ifPresent(x -> this.sideEffectKeys.add(x.getKey())); // TODO: when no more MapReducers, you can remove thisi
        }

        this.memoryComputeKeys.add(MemoryComputeKey.of(TraverserMapReduce.TRAVERSERS, MemoryComputeKey.setOperator(), false, false));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.traversal.storeState(configuration, TRAVERSAL);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
        for (final ReducingBarrierStep<?, ?> reducingBarrierStep : TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, this.traversal.get())) {
            memory.set(ReducingBarrierStep.REDUCING, reducingBarrierStep.getSeedSupplier().get());
        }
        for (final RangeGlobalStep<?> reducingBarrierStep : TraversalHelper.getStepsOfAssignableClassRecursively(RangeGlobalStep.class, this.traversal.get())) {
            memory.set(reducingBarrierStep.getId(), new TraverserSet<>());
        }
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final Memory memory) {
        this.traversal.get().getSideEffects().setLocalVertex(vertex);
        if (memory.isInitialIteration()) {    // ITERATION 1
            final TraverserSet<Object> haltedTraversers = vertex.<TraverserSet<Object>>property(HALTED_TRAVERSERS).orElse(new TraverserSet<>());
            vertex.property(VertexProperty.Cardinality.single, HALTED_TRAVERSERS, haltedTraversers);
            final TraverserSet<Object> aliveTraverses = new TraverserSet<>();
            IteratorUtils.removeOnNext(haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.get().getStartStep().getId());
                aliveTraverses.add((Traverser.Admin) traverser);
            });
            assert haltedTraversers.isEmpty();
            if (this.traversal.get().getStartStep() instanceof GraphStep) {
                final GraphStep<?, ?> graphStep = (GraphStep<Element, Element>) this.traversal.get().getStartStep();
                graphStep.reset();
                aliveTraverses.forEach(traverser -> graphStep.addStart((Traverser.Admin) traverser));
                aliveTraverses.clear();
                if (graphStep.returnsVertex())
                    graphStep.setIteratorSupplier(() -> ElementHelper.idExists(vertex.id(), graphStep.getIds()) ? (Iterator) IteratorUtils.of(vertex) : EmptyIterator.instance());
                else
                    graphStep.setIteratorSupplier(() -> (Iterator) IteratorUtils.filter(vertex.edges(Direction.OUT), edge -> ElementHelper.idExists(edge.id(), graphStep.getIds())));
                graphStep.forEachRemaining(traverser -> {
                    if (traverser.asAdmin().isHalted()) {
                        traverser.asAdmin().detach();
                        haltedTraversers.add((Traverser.Admin) traverser);
                    } else
                        aliveTraverses.add((Traverser.Admin) traverser);
                });
            }
            memory.add(VOTE_TO_HALT, aliveTraverses.isEmpty() || TraverserExecutor.execute(vertex, new SingleMessenger<>(messenger, aliveTraverses), this.traversalMatrix, memory));
        } else {  // ITERATION 1+
            memory.add(VOTE_TO_HALT, TraverserExecutor.execute(vertex, messenger, this.traversalMatrix, memory));
        }
        this.traversal.get().getSideEffects().forEach((key, value) -> {
            if (this.sideEffectKeys.contains(key) && vertex.<Map<String, Object>>property(VertexTraversalSideEffects.SIDE_EFFECTS).value().containsKey(key)) {
                memory.add(key, value);
                vertex.<Map<String, Object>>property(VertexTraversalSideEffects.SIDE_EFFECTS).value().remove(key);
            }
        });

    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            for (final GraphComputing<Object> graphComputing : TraversalHelper.getStepsOfAssignableClassRecursively(GraphComputing.class, this.traversal.get())) {
                graphComputing.getMemoryComputeKey().ifPresent(memoryKey -> {
                    final String key = memoryKey.getKey();
                    if (memory.exists(key)) {
                        if(graphComputing instanceof RangeGlobalStep) {
                            memory.set(TraverserMapReduce.TRAVERSERS, ((TraverserSet)graphComputing.generateFinalResult(memory.get(key))).iterator());
                        } else {
                            memory.set(key, graphComputing.generateFinalResult(memory.get(key)));
                        }

                    }
                });
            }
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
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
    public Optional<MessageCombiner<TraverserSet<?>>> getMessageCombiner() {
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

        public Builder traversal(final Traversal.Admin<?, ?> traversal) {
            PureTraversal.storeState(this.configuration, TRAVERSAL, traversal);
            return this;
        }
    }

}