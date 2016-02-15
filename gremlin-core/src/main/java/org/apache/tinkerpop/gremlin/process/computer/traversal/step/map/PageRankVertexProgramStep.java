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
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.VertexComputing;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PageRankVertexProgramStep extends AbstractStep<ComputerResult, ComputerResult> implements VertexComputing, TraversalParent, ByModulating {

    private transient Function<Graph, GraphComputer> graphComputerFunction = Graph::compute;

    private Traversal.Admin<Vertex, Edge> pageRankTraversal;
    private Traversal.Admin<Vertex, Edge> purePageRankTraversal;
    private boolean first = true;


    public PageRankVertexProgramStep(final Traversal.Admin traversal) {
        super(traversal);
        this.modulateBy(__.<Vertex>outE().asAdmin());
    }

    @Override
    protected Traverser<ComputerResult> processNextStart() throws NoSuchElementException {
        try {
            if (this.first && this.getPreviousStep() instanceof EmptyStep) {
                this.first = false;
                final Graph graph = this.getTraversal().getGraph().get();
                final GraphComputer graphComputer = this.graphComputerFunction.apply(graph).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW);
                return this.traversal.getTraverserGenerator().generate(graphComputer.program(PageRankVertexProgram.build().traversal(this.compileTraversal(graph)).create(graph)).submit().get(), this, 1l);
            } else {
                final Traverser.Admin<ComputerResult> traverser = this.starts.next();
                final Graph graph = traverser.get().graph();
                final GraphComputer graphComputer = this.graphComputerFunction.apply(graph).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW);
                return traverser.split(graphComputer.program(PageRankVertexProgram.build().traversal(this.compileTraversal(graph)).create(graph)).submit().get(), this);
            }
        } catch (final InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> localChildTraversal) {
        this.pageRankTraversal = this.integrateChild((Traversal.Admin) localChildTraversal);
        this.purePageRankTraversal = this.pageRankTraversal.clone();
    }

    @Override
    public List<Traversal.Admin<Vertex, Edge>> getLocalChildren() {
        return Collections.singletonList(this.pageRankTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.pageRankTraversal);
    }

    @Override
    public void setGraphComputerFunction(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }

    private final Traversal.Admin<Vertex, Edge> compileTraversal(final Graph graph) {
        final Traversal.Admin<Vertex, Edge> compiledPageRankTraversal = this.purePageRankTraversal.clone();
        compiledPageRankTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
        compiledPageRankTraversal.applyStrategies();
        return compiledPageRankTraversal;
    }

    @Override
    public PageRankVertexProgramStep clone() {
        final PageRankVertexProgramStep clone = (PageRankVertexProgramStep) super.clone();
        clone.pageRankTraversal = clone.integrateChild(this.pageRankTraversal);
        clone.purePageRankTraversal = clone.integrateChild(this.purePageRankTraversal);
        return clone;
    }
}
