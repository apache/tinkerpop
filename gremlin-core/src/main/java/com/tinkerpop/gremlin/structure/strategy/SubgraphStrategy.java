package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
            if (!testVertex(v)) {
                throw Graph.Exceptions.elementNotFound(Vertex.class, id);
            }

            return v;
        };
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (id) -> {
            final Edge e = f.apply(id);

            if (!testEdge(e)) {
                throw Graph.Exceptions.elementNotFound(Edge.class, id);
            }

            return e;
        };
    }

    public UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (direction, labels) ->
                new ChainIterator(
                        new PredicateIterator<>(ctx.getCurrent().getBaseVertex().iterators().edgeIterator(direction, labels)),
                        e -> {
                            if (direction.equals(Direction.BOTH)) {
                                return notStart(ctx.getCurrent(), e);
                            } else
                                return e.iterators().vertexIterator(direction.opposite());
                        });


    }

    public UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (direction, labels) -> new PredicateIterator<>(f.apply(direction, labels));
    }

    public UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return (f) -> direction -> new PredicateIterator<>(f.apply(direction));
    }

    public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> f.get().filter(t -> this.testVertex(t.get()));
    }

    public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getGraphEStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> () -> f.get().filter(t -> this.testEdge(t.get()));
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

    public Iterator<Vertex> notStart(final Vertex start, final Edge edge) {
        if (ElementHelper.areEqual(start, edge.iterators().vertexIterator(Direction.IN).next())) {
            return edge.iterators().vertexIterator(Direction.OUT);
        } else {
            return edge.iterators().vertexIterator(Direction.IN);
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    private final class PredicateIterator<E extends Element> implements Iterator<E> {

        private E nextUp;
        private final Iterator<E> iterator;

        public PredicateIterator(final Iterator<E> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (null != this.nextUp) {
                return true;
            } else {
                return this.advanceToNext();
            }
        }

        @Override
        public E next() {
            try {
                if (null != this.nextUp) {
                    return this.nextUp;
                } else {
                    if (this.advanceToNext())
                        return this.nextUp;
                    else
                        throw FastNoSuchElementException.instance();
                }
            } finally {
                this.nextUp = null;
            }
        }

        public boolean advanceToNext() {
            this.nextUp = null;
            while (this.iterator.hasNext()) {
                final E element = this.iterator.next();
                if (element instanceof Vertex) {
                    if (testVertex((Vertex) element)) {
                        this.nextUp = element;
                        return true;
                    }
                } else if (element instanceof Edge) {
                    if (testEdge((Edge) element)) {
                        this.nextUp = element;
                        return true;
                    }
                } else {
                    throw new UnsupportedOperationException("This iterator only filters vertices and edges for now: " + element.getClass());
                }
            }
            return false;
        }
    }

    public class ChainIterator implements Iterator<Vertex> {

        private final Iterator<Vertex> iterator;

        public ChainIterator(final Iterator<Edge> first, final Function<Edge, Iterator<Vertex>> mapFunction) {
            final List<Vertex> list = new ArrayList<>();
            while (first.hasNext()) {
                mapFunction.apply(first.next()).forEachRemaining(list::add);
            }
            this.iterator = new PredicateIterator(list.iterator());
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Vertex next() {
            return this.iterator.next();
        }


    }


}
