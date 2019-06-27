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
package org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram implements VertexProgram<Double> {


    public static final String PAGE_RANK = "gremlin.pageRankVertexProgram.pageRank";
    private static final String EDGE_COUNT = "gremlin.pageRankVertexProgram.edgeCount";
    private static final String PROPERTY = "gremlin.pageRankVertexProgram.property";
    private static final String VERTEX_COUNT = "gremlin.pageRankVertexProgram.vertexCount";
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String EPSILON = "gremlin.pageRankVertexProgram.epsilon";
    private static final String MAX_ITERATIONS = "gremlin.pageRankVertexProgram.maxIterations";
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String INITIAL_RANK_TRAVERSAL = "gremlin.pageRankVertexProgram.initialRankTraversal";
    private static final String TELEPORTATION_ENERGY = "gremlin.pageRankVertexProgram.teleportationEnergy";
    private static final String CONVERGENCE_ERROR = "gremlin.pageRankVertexProgram.convergenceError";

    private MessageScope.Local<Double> incidentMessageScope = MessageScope.Local.of(__::outE);
    private MessageScope.Local<Double> countMessageScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));
    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private PureTraversal<Vertex, ? extends Number> initialRankTraversal = null;
    private double alpha = 0.85d;
    private double epsilon = 0.00001d;
    private int maxIterations = 20;
    private String property = PAGE_RANK;
    private Set<VertexComputeKey> vertexComputeKeys;
    private Set<MemoryComputeKey> memoryComputeKeys;

    private PageRankVertexProgram() {

    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (configuration.containsKey(INITIAL_RANK_TRAVERSAL))
            this.initialRankTraversal = PureTraversal.loadState(configuration, INITIAL_RANK_TRAVERSAL, graph);
        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
            this.incidentMessageScope = MessageScope.Local.of(() -> this.edgeTraversal.get().clone());
            this.countMessageScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));
        }
        this.alpha = configuration.getDouble(ALPHA, this.alpha);
        this.epsilon = configuration.getDouble(EPSILON, this.epsilon);
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 20);
        this.property = configuration.getString(PROPERTY, PAGE_RANK);
        this.vertexComputeKeys = new HashSet<>(Arrays.asList(
                VertexComputeKey.of(this.property, false),
                VertexComputeKey.of(EDGE_COUNT, true)));
        this.memoryComputeKeys = new HashSet<>(Arrays.asList(
                MemoryComputeKey.of(TELEPORTATION_ENERGY, Operator.sum, true, true),
                MemoryComputeKey.of(VERTEX_COUNT, Operator.sum, true, true),
                MemoryComputeKey.of(CONVERGENCE_ERROR, Operator.sum, false, true)));
    }

    @Override
    public void storeState(final Configuration configuration) {
        VertexProgram.super.storeState(configuration);
        configuration.setProperty(ALPHA, this.alpha);
        configuration.setProperty(EPSILON, this.epsilon);
        configuration.setProperty(PROPERTY, this.property);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        if (null != this.edgeTraversal)
            this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        if (null != this.initialRankTraversal)
            this.initialRankTraversal.storeState(configuration, INITIAL_RANK_TRAVERSAL);
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return this.vertexComputeKeys;
    }

    @Override
    public Optional<MessageCombiner<Double>> getMessageCombiner() {
        return (Optional) PageRankMessageCombiner.instance();
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(memory.isInitialIteration() ? this.countMessageScope : this.incidentMessageScope);
        return set;
    }

    @Override
    public PageRankVertexProgram clone() {
        try {
            final PageRankVertexProgram clone = (PageRankVertexProgram) super.clone();
            if (null != this.initialRankTraversal)
                clone.initialRankTraversal = this.initialRankTraversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(TELEPORTATION_ENERGY, null == this.initialRankTraversal ? 1.0d : 0.0d);
        memory.set(VERTEX_COUNT, 0.0d);
        memory.set(CONVERGENCE_ERROR, 1.0d);
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Double> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            messenger.sendMessage(this.countMessageScope, 1.0d);
            memory.add(VERTEX_COUNT, 1.0d);
        } else {
            final double vertexCount = memory.<Double>get(VERTEX_COUNT);
            final double edgeCount;
            double pageRank;
            if (1 == memory.getIteration()) {
                edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0.0d, (a, b) -> a + b);
                vertex.property(VertexProperty.Cardinality.single, EDGE_COUNT, edgeCount);
                pageRank = null == this.initialRankTraversal ?
                        0.0d :
                        TraversalUtil.apply(vertex, this.initialRankTraversal.get()).doubleValue();
            } else {
                edgeCount = vertex.value(EDGE_COUNT);
                pageRank = IteratorUtils.reduce(messenger.receiveMessages(), 0.0d, (a, b) -> a + b);
            }
            //////////////////////////
            final double teleporationEnergy = memory.get(TELEPORTATION_ENERGY);
            if (teleporationEnergy > 0.0d) {
                final double localTerminalEnergy = teleporationEnergy / vertexCount;
                pageRank = pageRank + localTerminalEnergy;
                memory.add(TELEPORTATION_ENERGY, -localTerminalEnergy);
            }
            final double previousPageRank = vertex.<Double>property(this.property).orElse(0.0d);
            memory.add(CONVERGENCE_ERROR, Math.abs(pageRank - previousPageRank));
            vertex.property(VertexProperty.Cardinality.single, this.property, pageRank);
            memory.add(TELEPORTATION_ENERGY, (1.0d - this.alpha) * pageRank);
            pageRank = this.alpha * pageRank;
            if (edgeCount > 0.0d)
                messenger.sendMessage(this.incidentMessageScope, pageRank / edgeCount);
            else
                memory.add(TELEPORTATION_ENERGY, pageRank);
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        boolean terminate = memory.<Double>get(CONVERGENCE_ERROR) < this.epsilon || memory.getIteration() >= this.maxIterations;
        memory.set(CONVERGENCE_ERROR, 0.0d);
        return terminate;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "alpha=" + this.alpha + ", epsilon=" + this.epsilon + ", iterations=" + this.maxIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public final static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(PageRankVertexProgram.class);
        }

        public Builder iterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        public Builder alpha(final double alpha) {
            this.configuration.setProperty(ALPHA, alpha);
            return this;
        }

        public Builder property(final String key) {
            this.configuration.setProperty(PROPERTY, key);
            return this;
        }

        public Builder epsilon(final double epsilon) {
            this.configuration.setProperty(EPSILON, epsilon);
            return this;
        }

        public Builder edges(final Traversal.Admin<Vertex, Edge> edgeTraversal) {
            PureTraversal.storeState(this.configuration, EDGE_TRAVERSAL, edgeTraversal);
            return this;
        }

        public Builder initialRank(final Traversal.Admin<Vertex, ? extends Number> initialRankTraversal) {
            PureTraversal.storeState(this.configuration, INITIAL_RANK_TRAVERSAL, initialRankTraversal);
            return this;
        }
    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }
}