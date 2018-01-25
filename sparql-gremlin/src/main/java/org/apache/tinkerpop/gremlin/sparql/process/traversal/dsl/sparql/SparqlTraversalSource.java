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
package org.apache.tinkerpop.gremlin.sparql.process.traversal.dsl.sparql;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * A {@link TraversalSource} implementation that spawns {@link SparqlTraversal} instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SparqlTraversalSource implements TraversalSource {
    protected transient RemoteConnection connection;
    protected final Graph graph;
    protected TraversalStrategies strategies;
    protected Bytecode bytecode = new Bytecode();

    public SparqlTraversalSource(final Graph graph, final TraversalStrategies traversalStrategies) {
        this.graph = graph;
        this.strategies = traversalStrategies;
    }

    public SparqlTraversalSource(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }

    @Override
    public Bytecode getBytecode() {
        return this.bytecode;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public SparqlTraversalSource clone() {
        try {
            final SparqlTraversalSource clone = (SparqlTraversalSource) super.clone();
            clone.strategies = this.strategies.clone();
            clone.bytecode = this.bytecode.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //// CONFIGURATIONS

    @Override
    public SparqlTraversalSource withStrategies(final TraversalStrategy... traversalStrategies) {
        return (SparqlTraversalSource) TraversalSource.super.withStrategies(traversalStrategies);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public SparqlTraversalSource withoutStrategies(final Class<? extends TraversalStrategy>... traversalStrategyClasses) {
        return (SparqlTraversalSource) TraversalSource.super.withoutStrategies(traversalStrategyClasses);
    }

    @Override
    public SparqlTraversalSource withRemote(final Configuration conf) {
        return (SparqlTraversalSource) TraversalSource.super.withRemote(conf);
    }

    @Override
    public SparqlTraversalSource withRemote(final String configFile) throws Exception {
        return (SparqlTraversalSource) TraversalSource.super.withRemote(configFile);
    }

    @Override
    public SparqlTraversalSource withRemote(final RemoteConnection connection) {
        try {
            // check if someone called withRemote() more than once, so just release resources on the initial
            // connection as you can't have more than one. maybe better to toss IllegalStateException??
            if (this.connection != null)
                this.connection.close();
        } catch (Exception ignored) {
            // not sure there's anything to do here
        }

        this.connection = connection;
        final TraversalSource clone = this.clone();
        clone.getStrategies().addStrategies(new RemoteStrategy(connection));
        return (SparqlTraversalSource) clone;
    }

    /**
     * The start step for a SPARQL based traversal that accepts a string representation of the query to execute.
     */
    public <S> SparqlTraversal<S,String> sparql(final String query) {
        final SparqlTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.inject);
        clone.bytecode.addStep(GraphTraversal.Symbols.constant, query);
        final SparqlTraversal.Admin<S, S> traversal = new DefaultSparqlTraversal<>(clone);
        traversal.addStep(new InjectStep<S>(traversal));
        return traversal.addStep(new ConstantStep<S,String>(traversal, query));
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) connection.close();
    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
