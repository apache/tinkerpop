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
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce.TraverserMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;


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
    public static final String TRAVERSAL_SUPPLIER = "gremlin.traversalVertexProgram.traversalSupplier";

    // TODO: if not an adjacent traversal, use Local message scope -- a dual messaging system.
    private static final Set<MessageScope> MESSAGE_SCOPES = new HashSet<>(Collections.singletonList(MessageScope.Global.instance()));
    private static final Set<String> ELEMENT_COMPUTE_KEYS = new HashSet<>(Arrays.asList(HALTED_TRAVERSERS, TraversalSideEffects.SIDE_EFFECTS));
    private static final Set<String> MEMORY_COMPUTE_KEYS = new HashSet<>(Collections.singletonList(VOTE_TO_HALT));

    private LambdaHolder<Supplier<Traversal.Admin<?, ?>>> traversalSupplier;
    private Traversal.Admin<?, ?> traversal;
    private TraversalMatrix<?, ?> traversalMatrix;

    private final Set<MapReduce> mapReducers = new HashSet<>();

    private TraversalVertexProgram() {
    }

    /**
     * A helper method to yield a {@link Supplier} of {@link Traversal} from the {@link Configuration}.
     * The supplier is either a {@link Class}, {@link org.apache.tinkerpop.gremlin.process.computer.util.ScriptEngineLambda}, or a direct Java8 lambda.
     *
     * @param configuration The configuration containing the public static TRAVERSAL_SUPPLIER key.
     * @return the traversal supplier in the configuration
     */
    public static Supplier<Traversal.Admin<?, ?>> getTraversalSupplier(final Configuration configuration) {
        return LambdaHolder.<Supplier<Traversal.Admin<?, ?>>>loadState(configuration, TraversalVertexProgram.TRAVERSAL_SUPPLIER).get();
    }

    public Traversal.Admin<?, ?> getTraversal() {
        return this.traversal;
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversalSupplier = LambdaHolder.loadState(configuration, TRAVERSAL_SUPPLIER);
        if (null == this.traversalSupplier) {
            throw new IllegalArgumentException("The configuration does not have a traversal supplier");
        }
        this.traversal = this.traversalSupplier.get().get();
        if (!this.traversal.isLocked()) this.traversal.applyStrategies();
        ((ComputerResultStep) this.traversal.getEndStep()).byPass();
        this.traversalMatrix = new TraversalMatrix<>(this.traversal);
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal)) {
            this.mapReducers.add(mapReducer.getMapReduce());
        }
        if (!(this.traversal.getEndStep().getPreviousStep() instanceof SideEffectCapStep) && !(this.traversal.getEndStep().getPreviousStep() instanceof ReducingBarrierStep))
            this.mapReducers.add(new TraverserMapReduce(this.traversal.getEndStep().getPreviousStep()));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        this.traversalSupplier.storeState(configuration);
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
            final TraverserSet<Object> haltedTraversers = new TraverserSet<>();
            vertex.property(VertexProperty.Cardinality.single, HALTED_TRAVERSERS, haltedTraversers);

            if (!(this.traversal.getStartStep() instanceof GraphStep))
                throw new UnsupportedOperationException("TraversalVertexProgram currently only supports GraphStep starts on vertices or edges");

            final GraphStep<Element> graphStep = (GraphStep<Element>) this.traversal.getStartStep();
            final String future = graphStep.getNextStep().getId();
            final TraverserGenerator traverserGenerator = this.traversal.getTraverserGenerator();
            if (graphStep.returnsVertices()) {  // VERTICES (process the first step locally)
                if (ElementHelper.idExists(vertex.id(), graphStep.getIds())) {
                    final Traverser.Admin<Element> traverser = traverserGenerator.generate(vertex, graphStep, 1l);
                    traverser.setStepId(future);
                    traverser.detach();
                    if (traverser.isHalted())
                        haltedTraversers.add((Traverser.Admin) traverser);
                    else
                        memory.and(VOTE_TO_HALT, TraverserExecutor.execute(vertex, new SingleMessenger<>(messenger, new TraverserSet<>(traverser)), this.traversalMatrix));
                }
            } else {  // EDGES (process the first step via a message pass)
                boolean voteToHalt = true;
                final Iterator<Edge> starts = vertex.edges(Direction.OUT);
                while (starts.hasNext()) {
                    final Edge start = starts.next();
                    if (ElementHelper.idExists(start.id(), graphStep.getIds())) {
                        final Traverser.Admin<Element> traverser = traverserGenerator.generate(start, graphStep, 1l);
                        traverser.setStepId(future);
                        traverser.detach();
                        if (traverser.isHalted())
                            haltedTraversers.add((Traverser.Admin) traverser);
                        else {
                            voteToHalt = false;
                            messenger.sendMessage(MessageScope.Global.of(vertex), new TraverserSet<>(traverser));
                        }
                    }
                }
                memory.and(VOTE_TO_HALT, voteToHalt);
            }
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

    public <S, E> Traversal.Admin<S, E> computerResultTraversal(final ComputerResult result) {
        final Traversal.Admin<S, E> traversal = (Traversal.Admin<S, E>) this.getTraversal();
        ((ComputerResultStep) traversal.getEndStep()).populateTraversers(result);
        return traversal;
    }

    //////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        public Builder() {
            super(TraversalVertexProgram.class);
        }

        public Builder traversal(final Class<? extends Graph> graphClass, final TraversalSource.Builder builder, final String scriptEngine, final String traversalScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SERIALIZED_OBJECT, TRAVERSAL_SUPPLIER, new TraversalScriptSupplier<>(graphClass, builder, scriptEngine, traversalScript));
            return this;
        }

        public Builder traversal(final Traversal.Admin<?, ?> traversal) {
            return this.traversal(traversal, true);
        }

        public Builder traversal(final Traversal.Admin<?, ?> traversal, boolean serialize) {
            if (serialize)
                LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SERIALIZED_OBJECT, TRAVERSAL_SUPPLIER, new TraversalSupplier<>(traversal, false));
            else
                LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, TRAVERSAL_SUPPLIER, new TraversalSupplier<>(traversal, true));
            return this;
        }

        public Builder traversal(final Class<Supplier<Traversal.Admin>> traversalClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, TRAVERSAL_SUPPLIER, traversalClass);
            return this;
        }

        // TODO Builder resolveElements(boolean) to be fed to ComputerResultStep
    }

}