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
package com.apache.tinkerpop.gremlin.structure.strategy;

import com.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import com.apache.tinkerpop.gremlin.structure.Direction;
import com.apache.tinkerpop.gremlin.structure.Edge;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import com.apache.tinkerpop.gremlin.structure.util.StringFactory;
import com.apache.tinkerpop.gremlin.util.StreamFactory;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * A {@link GraphStrategy} which creates a logical subgraph to selectively include vertices and edges of a
 * {@link com.apache.tinkerpop.gremlin.structure.Graph} according to provided criteria.  A vertex is in the subgraph if
 * it meets the specified {@link #vertexPredicate}.  An edge is in the subgraph if it meets the specified
 * {@link #edgePredicate} and its associated vertices meet the specified {@link #vertexPredicate}.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SubgraphStrategy implements GraphStrategy {

    private final Predicate<Vertex> vertexPredicate;
    private final Predicate<Edge> edgePredicate;
    // TODO protected Predicate<VertexProperty> vertexPropertyPredicate;

    private SubgraphStrategy(final Predicate<Vertex> vertexPredicate, final Predicate<Edge> edgePredicate) {
        this.vertexPredicate = vertexPredicate;
        this.edgePredicate = edgePredicate;
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVertexIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (direction, labels) -> StreamFactory
                .stream(ctx.getCurrent().getBaseVertex().iterators().edgeIterator(direction, labels))
                .filter(this::testEdge)
                .map(edge -> otherVertex(direction, ctx.getCurrent(), edge))
                .filter(this::testVertex).iterator();
        // TODO: Note that we are not doing f.apply() like the other methods. Is this bad?
        // by not calling f.apply() to get the iterator, we're possibly bypassing strategy methods that
        // could have been sequenced
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (direction, labels) -> IteratorUtils.filter(f.apply(direction, labels), this::testEdge);
    }

    @Override
    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVertexIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return (f) -> direction -> IteratorUtils.filter(f.apply(direction), this::testVertex);
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return (f) -> ids -> f.apply(ids).filter(t -> this.testVertex(t.get())); // TODO: we should make sure index hits go first.
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Edge, Edge>>> getGraphEStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return (f) -> ids -> f.apply(ids).filter(t -> this.testEdge(t.get()));  // TODO: we should make sure index hits go first.
    }

    // TODO: make this work for DSL -- we need Element predicate
    /*public UnaryOperator<Supplier<GraphTraversal>> getGraphOfStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> f.get().filter(el);
    }*/


    private boolean testVertex(final Vertex vertex) {
        return vertexPredicate.test(vertex);
    }

    private boolean testEdge(final Edge edge) {
        // the edge must pass the edge predicate, and both of its incident vertices must also pass the vertex predicate
        // inV() and/or outV() will be empty if they do not.  it is sometimes the case that an edge is unwrapped
        // in which case it may not be filtered.  in such cases, the vertices on such edges should be tested.
        return edgePredicate.test(edge)
                && (edge instanceof StrategyWrapped ? edge.inV().hasNext() && edge.outV().hasNext()
                : testVertex(edge.inV().next()) && testVertex(edge.outV().next()));
    }

    private static final Vertex otherVertex(final Direction direction, final Vertex start, final Edge edge) {
        if (direction.equals(Direction.BOTH)) {
            final Vertex inVertex = edge.iterators().vertexIterator(Direction.IN).next();
            return ElementHelper.areEqual(start, inVertex) ?
                    edge.iterators().vertexIterator(Direction.OUT).next() :
                    inVertex;
        } else {
            return edge.iterators().vertexIterator(direction.opposite()).next();
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private Predicate<Vertex> vertexPredicate = v -> true;
        private Predicate<Edge> edgePredicate = v -> true;

        private Builder() {
        }

        public Builder vertexPredicate(final Predicate<Vertex> vertexPredicate) {
            this.vertexPredicate = vertexPredicate;
            return this;
        }

        public Builder edgePredicate(final Predicate<Edge> edgePredicate) {
            this.edgePredicate = edgePredicate;
            return this;
        }

        public SubgraphStrategy create() {
            return new SubgraphStrategy(vertexPredicate, edgePredicate);
        }
    }
}
