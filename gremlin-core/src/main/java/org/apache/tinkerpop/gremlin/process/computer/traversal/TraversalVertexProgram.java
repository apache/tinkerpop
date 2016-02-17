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
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
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
    private static final Set<String> ELEMENT_COMPUTE_KEYS = new HashSet<>(Arrays.asList(HALTED_TRAVERSERS, TraversalSideEffects.SIDE_EFFECTS));
    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Collections.singletonList(VOTE_TO_HALT));

    private PureTraversal<?, ?> pureTraversal;
    private Traversal.Admin<?, ?> traversal;
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
        return VertexProgram.<TraversalVertexProgram>createVertexProgram(graph, configuration).traversal;
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (!configuration.containsKey(TRAVERSAL))
            throw new IllegalArgumentException("The configuration does not have a traversal supplier: " + TRAVERSAL);
        this.pureTraversal = PureTraversal.loadState(configuration, TRAVERSAL, graph);
        this.traversal = this.pureTraversal.get();
        if (!this.traversal.isLocked())
            this.traversal.applyStrategies();
        this.traversalMatrix = new TraversalMatrix<>(this.traversal);
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal)) {
            this.mapReducers.add(mapReducer.getMapReduce());
        }
        if (!(this.traversal.getEndStep() instanceof SideEffectCapStep) && !(this.traversal.getEndStep() instanceof ReducingBarrierStep))
            this.mapReducers.add(new TraverserMapReduce(this.traversal));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.pureTraversal.storeState(configuration, TRAVERSAL);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return MESSAGE_SCOPES;
    }

    @Override
    public void execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final Memory memory) {
        this.traversal.getSideEffects().setLocalVertex(vertex);
        if (memory.isInitialIteration()) {    // ITERATION 1
            final TraverserSet<Object> haltedTraversers = vertex.<TraverserSet<Object>>property(HALTED_TRAVERSERS).orElse(new TraverserSet<>());
            vertex.property(VertexProperty.Cardinality.single, HALTED_TRAVERSERS, haltedTraversers);
            final TraverserSet<Object> aliveTraverses = new TraverserSet<>();
            IteratorUtils.removeOnNext(haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.getStartStep().getId());
                aliveTraverses.add((Traverser.Admin) traverser);
            });
            assert haltedTraversers.isEmpty();
            if (this.traversal.getStartStep() instanceof GraphStep) {
                final GraphStep<?, ?> graphStep = (GraphStep<Element, Element>) this.traversal.getStartStep();
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
            memory.and(VOTE_TO_HALT, aliveTraverses.isEmpty() || TraverserExecutor.execute(vertex, new SingleMessenger<>(messenger, aliveTraverses), this.traversalMatrix));
        } else {  // ITERATION 1+
            memory.and(VOTE_TO_HALT, TraverserExecutor.execute(vertex, messenger, this.traversalMatrix));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return ELEMENT_COMPUTE_KEYS;
    }

    @Override
    public Set<String> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
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
            clone.traversalMatrix = new TraversalMatrix<>(clone.traversal);
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
        final String traversalString = this.traversal.toString().substring(1);
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