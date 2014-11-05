package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.EmptyGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
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

    public SubgraphStrategy(final Predicate<Vertex> vertexPredicate, final Predicate<Edge> edgePredicate) {
        this.vertexPredicate = vertexPredicate;
        this.edgePredicate = edgePredicate;
    }

    @Override
    public GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
        traversal.getStrategies().register(new SubgraphTraversalStrategy());
        return traversal;
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
        return element instanceof Vertex
                ? testVertex((Vertex) element)
                : testEdge((Edge) element);
    }

    @Override
    public String toString() {
        return SubgraphStrategy.class.getSimpleName().toLowerCase();
    }

    public class SubgraphTraversalStrategy implements TraversalStrategy.NoDependencies {

        @Override
        public void apply(final Traversal traversal, final TraversalEngine traversalEngine) {
            // modify the traversal by appending filters after some steps, replacing others.  the idea is to
            // find VertexStep instances and replace them with SubgraphVertexStep. after each GraphStep,
            // EdgeVertexStep insert a SubgraphFilterStep.
            final List<Class> insertAfterSteps = Arrays.<Class>asList(GraphStep.class, EdgeVertexStep.class);
            final List<Integer> insertAfterPositions = new ArrayList<>();
            final List<Integer> replacePositions = new ArrayList<>();
            final List<Step> traversalSteps = traversal.getSteps();
            for (int i = 0; i < traversalSteps.size(); i++) {
                final int pos = i;
                if (insertAfterSteps.stream().anyMatch(c -> c.isAssignableFrom(traversalSteps.get(pos).getClass()))) {
                    insertAfterPositions.add(i);
                }

                if (VertexStep.class.isAssignableFrom(traversalSteps.get(pos).getClass())) {
                    replacePositions.add(i);
                }
            }

            for (int pos : replacePositions) {
                final VertexStep other = (VertexStep) traversalSteps.get(pos);
                TraversalHelper.replaceStep(traversalSteps.get(pos), new SubgraphVertexStep(other), traversal);
            }

            Collections.reverse(insertAfterPositions);
            for (int pos : insertAfterPositions) {
                TraversalHelper.insertStep(new SubgraphFilterStep(traversal), pos + 1, traversal);
            }
        }
    }

    /**
     * A step that checks the subgraph filters to ensure that a {@link Element} should pass through.
     */
    private class SubgraphFilterStep extends FilterStep<Element> implements Reversible {

        public SubgraphFilterStep(final Traversal traversal) {
            super(traversal);
            this.setPredicate(traverser -> testElement(traverser.get()));
        }

        public String toString() {
            return TraversalHelper.makeStepString(this, vertexPredicate, edgePredicate);
        }
    }

    /**
     * A step that wraps up the adjacent traversal from the vertex allowing the results to be iterated within the
     * context of the subgraph filters.
     */
    private class SubgraphVertexStep<E extends Element> extends VertexStep<E> {

        public SubgraphVertexStep(final VertexStep<E> other) {
            this(other.getTraversal(), other.getReturnClass(), other.getDirection(), other.getEdgeLabels());
        }

        public SubgraphVertexStep(final Traversal traversal,
                                  final Class<E> returnClass,
                                  final Direction direction,
                                  final String... edgeLabels) {
            super(traversal, returnClass, direction, edgeLabels);
            this.setFunction(traverser -> {
                final Vertex nextVertex = traverser.get();
                if (testVertex(nextVertex)) {

                    Iterator<E> iter = null;

                    if (Vertex.class.isAssignableFrom(returnClass)) {
                        Iterator<Vertex> vertexIter = null;
                        switch (direction) {
                            case OUT:
                                vertexIter = new EdgeVertexIterator(Direction.OUT, nextVertex.outE(edgeLabels));
                                break;
                            case IN:
                                vertexIter = new EdgeVertexIterator(Direction.IN, nextVertex.inE(edgeLabels));
                                break;
                            case BOTH:
                                vertexIter = new MultiIterator<>(
                                        new EdgeVertexIterator(Direction.IN, nextVertex.inE(edgeLabels)),
                                        new EdgeVertexIterator(Direction.OUT, nextVertex.outE(edgeLabels)));
                                break;
                        }

                        iter = (Iterator<E>) vertexIter;
                    } else {
                        Iterator<Edge> edgeIter = null;
                        switch (direction) {
                            case OUT:
                                edgeIter = nextVertex.outE(edgeLabels);
                                break;
                            case IN:
                                edgeIter = nextVertex.inE(edgeLabels);
                                break;
                            case BOTH:
                                edgeIter = nextVertex.bothE(edgeLabels);
                                break;
                        }

                        edgeIter = new EdgeIterator(edgeIter);
                        iter = (Iterator<E>) edgeIter;
                    }

                    return iter;
                } else {
                    return EmptyGraphTraversal.instance();
                }
            });
        }

        /*public String toString() {
            return TraversalHelper.makeStepString(this, this.direction);
        }*/
    }

    private class EdgeIterator implements Iterator<Edge> {
        private final Iterator<Edge> baseIterator;

        private Edge nextElement;

        private EdgeIterator(final Iterator<Edge> baseIterator) {
            this.baseIterator = baseIterator;
            advanceToNext();
        }

        @Override
        public boolean hasNext() {
            return null != nextElement;
        }

        @Override
        public Edge next() {
            if (null == nextElement) throw new NoSuchElementException();
            final Edge tmp = nextElement;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            while (baseIterator.hasNext()) {
                final Edge nextBaseElement = baseIterator.next();
                if (testEdge(nextBaseElement)) {
                    nextElement = nextBaseElement;
                    return;
                }
            }

            nextElement = null;
        }
    }

    private class EdgeVertexIterator implements Iterator<Vertex> {
        private final Direction direction;

        private final Iterator<Edge> edgeIterator;
        private Iterator<Vertex> vertexIterator;

        private Edge nextEdge;
        private Vertex nextVertex;

        private EdgeVertexIterator(final Direction direction,
                                   final Iterator<Edge> baseIterator) {
            if (direction == Direction.BOTH) throw new IllegalArgumentException();
            this.direction = direction;
            this.edgeIterator = baseIterator;
            advanceToNext();
        }

        @Override
        public boolean hasNext() {
            return null != nextVertex;
        }

        @Override
        public Vertex next() {
            if (null == nextVertex) throw new NoSuchElementException();
            final Vertex tmp = nextVertex;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            do {
                while (null != vertexIterator && vertexIterator.hasNext()) {
                    final Vertex nextBaseVertex = vertexIterator.next();
                    if (testVertex(nextBaseVertex)) {
                        nextVertex = nextBaseVertex;
                        return;
                    }
                }

                if (!edgeIterator.hasNext()) {
                    nextVertex = null;
                    return;
                }

                final Edge nextBaseEdge = edgeIterator.next();
                if (testEdge(nextBaseEdge)) {
                    nextEdge = nextBaseEdge;
                    switch (direction) {
                        case OUT:
                            vertexIterator = nextEdge.inV();
                            break;
                        case IN:
                            vertexIterator = nextEdge.outV();
                            break;
                    }
                }
            } while (true); // break out when the next vertex is found or the iterator is exhausted
        }
    }

    private class MultiIterator<V> implements Iterator<V> {
        private final Iterator<V>[] baseIterators;
        private int iteratorIndex;
        private V nextItem;

        private MultiIterator(final Iterator<V>... baseIterators) {
            this.baseIterators = baseIterators;
            if (0 == baseIterators.length) throw new IllegalArgumentException("must supply at least one base iterator");
            iteratorIndex = 0;
            advanceToNext();
        }

        @Override
        public boolean hasNext() {
            return null != nextItem;
        }

        @Override
        public V next() {
            if (null == nextItem) throw new NoSuchElementException();
            V tmp = nextItem;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            nextItem = null;

            do {
                if (iteratorIndex >= baseIterators.length)
                    return;

                if (baseIterators[iteratorIndex].hasNext()) {
                    nextItem = baseIterators[iteratorIndex].next();
                    return;
                }

                iteratorIndex++;
            } while (true);
        }
    }
}
