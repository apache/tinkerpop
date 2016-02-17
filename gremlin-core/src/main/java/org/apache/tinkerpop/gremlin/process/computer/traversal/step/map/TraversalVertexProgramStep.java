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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgramStep extends VertexProgramStep implements TraversalParent {

    private transient Function<Graph, GraphComputer> graphComputerFunction = Graph::compute;
    public PureTraversal<?, ?> computerTraversal;

    public TraversalVertexProgramStep(final Traversal.Admin traversal, final Traversal.Admin<?, ?> computerTraversal) {
        super(traversal);
        this.computerTraversal = new PureTraversal<>(computerTraversal);
        this.integrateChild(this.computerTraversal.get());
    }

    public List<Traversal.Admin<?, ?>> getGlobalChildren() {
        return Collections.singletonList(this.computerTraversal.get());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.computerTraversal.get());
    }

    @Override
    public TraversalVertexProgramStep clone() {
        final TraversalVertexProgramStep clone = (TraversalVertexProgramStep) super.clone();
        clone.computerTraversal = this.computerTraversal.clone();
        clone.integrateChild(this.computerTraversal.get());
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalParent.super.getSelfAndChildRequirements(TraverserRequirement.BULK);
    }

    @Override
    public void setGraphComputerFunction(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }

    @Override
    public TraversalVertexProgram generateProgram(final Graph graph) {
        this.computerTraversal.reset();
        final Traversal.Admin<?, ?> compiledComputerTraversal = this.computerTraversal.get();
        compiledComputerTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone());
        this.getTraversal().getStrategies().toList().forEach(compiledComputerTraversal.getStrategies()::addStrategies);
        compiledComputerTraversal.setSideEffects(this.getTraversal().getSideEffects());
        compiledComputerTraversal.setParent(this);
        compiledComputerTraversal.applyStrategies();
        return TraversalVertexProgram.build()
                .traversal(compiledComputerTraversal)
                .create(graph);
    }

    @Override
    public GraphComputer generateComputer(final Graph graph) {
        final GraphComputer graphComputer = this.graphComputerFunction.apply(graph);
        if (!(this.getNextStep() instanceof ComputerResultStep))
            graphComputer.persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW);
        return graphComputer;
    }
}
