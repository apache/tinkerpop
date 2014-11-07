package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A GraphStrategy which creates a logical subgraph to selectively include vertices and edges of a Graph according to
 * provided criteria.  A vertex is in the subgraph if it meets the specified {@link #vertexPredicate}.  An edge
 * is in the subgraph if it meets the specified {@link #edgePredicate} and its associated vertices meet the
 * specified {@link #vertexPredicate}.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphStrategy implements GraphStrategy {

    protected Predicate<Vertex> vertexPredicate;
    protected Predicate<Edge> edgePredicate;
    // TODO protected Predicate<VertexProperty> vertexPropertyPredicate;

    public SubgraphStrategy(final Predicate<Vertex> vertexPredicate, final Predicate<Edge> edgePredicate) {
        this.vertexPredicate = vertexPredicate;
        this.edgePredicate = edgePredicate;
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (id) -> {
            final Vertex v = f.apply(id);
            if (!this.testVertex(v)) {
                throw Graph.Exceptions.elementNotFound(Vertex.class, id);
            }
            return v;
        };
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (id) -> {
            final Edge e = f.apply(id);

            if (!this.testEdge(e)) {
                throw Graph.Exceptions.elementNotFound(Edge.class, id);
            }

            return e;
        };
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (direction, labels) -> StreamFactory
                .stream(ctx.getCurrent().edgeIterator(direction, labels))
                .filter(this::testEdge)
                .map(edge -> otherVertex(direction, ctx.getCurrent(), edge))
                .filter(this::testVertex)
                .map(v -> ((StrategyWrappedVertex) v).getBaseVertex()).iterator();
        // TODO: why do we have to unwrap? Note that we are not doing f.apply() like the other methods. Is this bad?
    }

    @Override
    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (direction, labels) -> StreamFactory.stream(f.apply(direction, labels)).filter(this::testEdge).iterator();
    }

    @Override
    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return (f) -> direction -> StreamFactory.stream(f.apply(direction)).filter(this::testVertex).iterator();
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> f.get().filter(t -> this.testVertex(t.get())); // TODO: we should make sure index hits go first.
    }

    @Override
    public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getGraphEStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> f.get().filter(t -> this.testEdge(t.get()));  // TODO: we should make sure index hits go first.
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

    private boolean testElement(final Element element) {
        return element instanceof Vertex ? testVertex((Vertex) element) : testEdge((Edge) element);
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
}
