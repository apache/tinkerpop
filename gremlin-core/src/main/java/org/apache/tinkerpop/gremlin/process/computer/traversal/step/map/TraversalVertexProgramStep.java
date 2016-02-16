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

package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgramStep extends AbstractStep<ComputerResult, ComputerResult> implements TraversalParent, VertexComputing {

    private transient Function<Graph, GraphComputer> graphComputerFunction = Graph::compute;
    public Traversal.Admin<?, ?> computerTraversal;
    public Traversal.Admin<?, ?> pureComputerTraversal;

    private boolean first = true;

    public TraversalVertexProgramStep(final Traversal.Admin traversal, final Traversal.Admin<?, ?> computerTraversal) {
        super(traversal);
        this.pureComputerTraversal = computerTraversal.clone();
        this.computerTraversal = this.integrateChild(computerTraversal);
    }

    public List<Traversal.Admin<?, ?>> getGlobalChildren() {
        return Collections.singletonList(this.computerTraversal);
    }

    @Override
    protected Traverser<ComputerResult> processNextStart() {
        try {
            if (this.first && this.getPreviousStep() instanceof EmptyStep) {
                this.first = false;
                final Graph graph = this.getTraversal().getGraph().get();
                final GraphComputer graphComputer = this.getComputer(graph);
                final ComputerResult result = graphComputer.program(TraversalVertexProgram.build().traversal(this.compileTraversal(graph)).create(graph)).submit().get();
                return this.getTraversal().getTraverserGenerator().generate(result, (Step) this, 1l);
            } else {
                final Traverser.Admin<ComputerResult> traverser = this.starts.next();
                final Graph graph = traverser.get().graph();
                final GraphComputer graphComputer = this.getComputer(graph);
                final ComputerResult result = graphComputer.program(TraversalVertexProgram.build().traversal(this.compileTraversal(graph)).create(graph)).submit().get();
                return traverser.split(result, this);
            }
        } catch (final InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.computerTraversal);
    }

    @Override
    public TraversalVertexProgramStep clone() {
        final TraversalVertexProgramStep clone = (TraversalVertexProgramStep) super.clone();
        clone.computerTraversal = this.integrateChild(this.computerTraversal.clone());
        clone.pureComputerTraversal = this.pureComputerTraversal.clone();
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalParent.super.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    private final GraphComputer getComputer(final Graph graph) {
        final GraphComputer graphComputer = this.graphComputerFunction.apply(graph);
        if (!(this.getNextStep() instanceof ComputerResultStep))
            graphComputer.persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW);
        return graphComputer;
    }

    private final Traversal.Admin<?, ?> compileTraversal(final Graph graph) {
        final Traversal.Admin<?, ?> compiledComputerTraversal = this.pureComputerTraversal.clone();
        compiledComputerTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone());
        this.getTraversal().getStrategies().toList().forEach(compiledComputerTraversal.getStrategies()::addStrategies);
        compiledComputerTraversal.setParent(this);
        compiledComputerTraversal.setSideEffects(this.computerTraversal.getSideEffects());
        compiledComputerTraversal.applyStrategies();
        return compiledComputerTraversal;
    }

    @Override
    public void setGraphComputerFunction(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }
}
