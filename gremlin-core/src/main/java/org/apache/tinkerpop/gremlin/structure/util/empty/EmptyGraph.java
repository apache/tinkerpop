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
package org.apache.tinkerpop.gremlin.structure.util.empty;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.util.EmptyGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyGraph implements Graph, Graph.Iterators {

    private static final String MESSAGE = "The graph is immutable and empty";
    private static final EmptyGraph INSTANCE = new EmptyGraph();

    private EmptyGraph() {

    }

    public static Graph instance() {
        return INSTANCE;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        return EmptyGraphTraversal.instance();
    }

    @Override
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        return EmptyGraphTraversal.instance();
    }

    @Override
    public <T extends Traversal<S, S>, S> T of(final Class<T> traversal) {
        return (T) EmptyTraversal.instance();
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public TraversalEngine engine() {
       return StandardTraversalEngine.instance();
    }

    @Override
    public void engine(final TraversalEngine engine) {

    }

    @Override
    public Transaction tx() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Variables variables() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Configuration configuration() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        return Collections.emptyIterator();
    }
}