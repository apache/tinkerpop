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
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PageRankVertexProgram extends StaticVertexProgram<Double> {

    private MessageScope.Local<Double> incidentMessageScope = MessageScope.Local.of(__::outE);
    private MessageScope.Local<Double> countMessageScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));

    public static final String PAGE_RANK = "gremlin.pageRankVertexProgram.pageRank";
    public static final String EDGE_COUNT = "gremlin.pageRankVertexProgram.edgeCount";

    private static final String VERTEX_COUNT = "gremlin.pageRankVertexProgram.vertexCount";
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String TOTAL_ITERATIONS = "gremlin.pageRankVertexProgram.totalIterations";
    private static final String INCIDENT_TRAVERSAL_SUPPLIER = "gremlin.pageRankVertexProgram.incidentTraversalSupplier";

    private LambdaHolder<Supplier<Traversal<Vertex, Edge>>> traversalSupplier;
    private double vertexCountAsDouble = 1.0d;
    private double alpha = 0.85d;
    private int totalIterations = 30;

    private static final Set<String> COMPUTE_KEYS = new HashSet<>(Arrays.asList(PAGE_RANK, EDGE_COUNT));

    private PageRankVertexProgram() {

    }

    @Override
    public void loadState(final Configuration configuration) {
        this.traversalSupplier = LambdaHolder.loadState(configuration, INCIDENT_TRAVERSAL_SUPPLIER);
        if (null != this.traversalSupplier) {
            this.incidentMessageScope = MessageScope.Local.of(this.traversalSupplier.get());
            this.countMessageScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.incidentMessageScope));
        }
        this.vertexCountAsDouble = configuration.getDouble(VERTEX_COUNT, 1.0d);
        this.alpha = configuration.getDouble(ALPHA, 0.85d);
        this.totalIterations = configuration.getInt(TOTAL_ITERATIONS, 30);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, PageRankVertexProgram.class.getName());
        configuration.setProperty(VERTEX_COUNT, this.vertexCountAsDouble);
        configuration.setProperty(ALPHA, this.alpha);
        configuration.setProperty(TOTAL_ITERATIONS, this.totalIterations);
        if (null != this.traversalSupplier) {
            this.traversalSupplier.storeState(configuration);
        }
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
    public Set<String> getElementComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public Optional<MessageCombiner<Double>> getMessageCombiner() {
        return (Optional) PageRankMessageCombiner.instance();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> set = new HashSet<>();
        set.add(memory.isInitialIteration() ? this.countMessageScope : this.incidentMessageScope);
        return set;
    }

    @Override
    public void setup(final Memory memory) {

    }

    @Override
    public void execute(final Vertex vertex, Messenger<Double> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            messenger.sendMessage(this.countMessageScope, 1.0d);
        } else if (1 == memory.getIteration()) {
            double initialPageRank = 1.0d / this.vertexCountAsDouble;
            double edgeCount = IteratorUtils.reduce(messenger.receiveMessages(this.countMessageScope), 0.0d, (a, b) -> a + b);
            vertex.property(PAGE_RANK, initialPageRank);
            vertex.property(EDGE_COUNT, edgeCount);
            messenger.sendMessage(this.incidentMessageScope, initialPageRank / edgeCount);
        } else {
            double newPageRank = IteratorUtils.reduce(messenger.receiveMessages(this.incidentMessageScope), 0.0d, (a, b) -> a + b);
            newPageRank = (this.alpha * newPageRank) + ((1.0d - this.alpha) / this.vertexCountAsDouble);
            vertex.property(PAGE_RANK, newPageRank);
            messenger.sendMessage(this.incidentMessageScope, newPageRank / vertex.<Double>value(EDGE_COUNT));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() >= this.totalIterations;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "alpha=" + this.alpha + ",iterations=" + this.totalIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(PageRankVertexProgram.class);
        }

        public Builder iterations(final int iterations) {
            this.configuration.setProperty(TOTAL_ITERATIONS, iterations);
            return this;
        }

        public Builder alpha(final double alpha) {
            this.configuration.setProperty(ALPHA, alpha);
            return this;
        }

        public Builder incident(final String scriptEngine, final String traversalScript) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.SCRIPT, INCIDENT_TRAVERSAL_SUPPLIER, new String[]{scriptEngine, traversalScript});
            return this;
        }

        public Builder incident(final String traversalScript) {
            return incident(GREMLIN_GROOVY, traversalScript);
        }

        public Builder incident(final Supplier<Traversal<Vertex, Edge>> traversal) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.OBJECT, INCIDENT_TRAVERSAL_SUPPLIER, traversal);
            return this;
        }

        public Builder incident(final Class<Supplier<Traversal<Vertex, Edge>>> traversalClass) {
            LambdaHolder.storeState(this.configuration, LambdaHolder.Type.CLASS, INCIDENT_TRAVERSAL_SUPPLIER, traversalClass);
            return this;
        }

        public Builder vertexCount(final long vertexCount) {
            this.configuration.setProperty(VERTEX_COUNT, (double) vertexCount);
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