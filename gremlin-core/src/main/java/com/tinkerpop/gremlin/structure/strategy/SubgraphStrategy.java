package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.util.EmptyTraversal;
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
 * A GraphStrategy which creates a logical subgraph to selectively include vertices and edges of a Graph according to provided criteria
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
        traversal.strategies().register(new SubgraphTraversalStrategy());
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
        // inV() and/or outV() will be empty if they do not
        return edgePredicate.test(edge) && edge.inV().hasNext() && edge.outV().hasNext(); // && vertexPredicate.test(edge.inV().next()) && vertexPredicate.test(edge.outV().next());
    }

    private boolean testElement(final Element element) {
        return element instanceof Vertex
                ? testVertex((Vertex) element)
                : testEdge((Edge) element);
    }

    @Override
    public String toString() {
        return SubgraphStrategy.class.getSimpleName();
    }

    private class SubgraphTraversalStrategy implements TraversalStrategy.NoDependencies {

        public void apply(final Traversal traversal) {
            // modify the traversal by appending filters after some steps, replacing others
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
                    VertexStep vs = (VertexStep) traversalSteps.get(pos);
                    if (Vertex.class.isAssignableFrom(vs.returnClass)) {
                        replacePositions.add(i);
                    } else {
                        insertAfterPositions.add(i);
                    }
                }
            }

            for (int pos : replacePositions) {
                VertexStep other = (VertexStep) traversalSteps.get(pos);
                TraversalHelper.replaceStep(traversalSteps.get(pos), new SubgraphVertexStep(other), traversal);
            }

            Collections.reverse(insertAfterPositions);
            for (int pos : insertAfterPositions) {
                TraversalHelper.insertStep(new SubgraphFilterStep(traversal), pos + 1, traversal);
            }
        }
    }

    private class SubgraphFilterStep extends FilterStep<Element> implements Reversible {

        public SubgraphFilterStep(final Traversal traversal) {
            super(traversal);
            this.setPredicate(traverser -> testElement(traverser.get()));
        }

        public String toString() {
            return TraversalHelper.makeStepString(this, vertexPredicate, edgePredicate);
        }
    }

    private class SubgraphVertexStep<E extends Element> extends FlatMapStep<Vertex, E> { // TODO: implement Reversible

        private final Direction direction;

        public SubgraphVertexStep(final VertexStep<E> other) {
            this(other.getTraversal(), other.returnClass, other.direction, other.branchFactor, other.labels);
        }

        public SubgraphVertexStep(final Traversal traversal,
                                  final Class<E> returnClass,
                                  final Direction direction,
                                  final int branchFactor,
                                  final String... labels) {
            super(traversal);
            this.direction = direction;
            this.setFunction(traverser -> {
                Vertex nextVertex = traverser.get();
                if (testVertex(nextVertex)) {

                    Iterator<E> iter = null;

                    if (Vertex.class.isAssignableFrom(returnClass)) {
                        Iterator<Vertex> vertexIter = null;
                        switch (direction) {
                            case OUT:
                                vertexIter = new EdgeVertexIterator(Direction.OUT, nextVertex.outE(labels));
                                break;
                            case IN:
                                vertexIter = new EdgeVertexIterator(Direction.IN, nextVertex.inE(labels));
                                break;
                            case BOTH:
                                vertexIter = new MultiIterator<>(
                                        new EdgeVertexIterator(Direction.IN, nextVertex.inE(labels)),
                                        new EdgeVertexIterator(Direction.OUT, nextVertex.outE(labels)));
                                break;
                        }

                        iter = (Iterator<E>) vertexIter;
                    } else {
                        Iterator<Edge> edgeIter = null;

                        switch (direction) {
                            case OUT:
                                edgeIter = nextVertex.outE(labels);
                                break;
                            case IN:
                                edgeIter = nextVertex.inE(labels);
                                break;
                            case BOTH:
                                edgeIter = nextVertex.bothE(labels);
                                break;
                        }

                        edgeIter = new EdgeIterator(edgeIter);

                        iter = (Iterator<E>) edgeIter;
                    }

                    if (branchFactor > 0) {
                        iter = new BranchFactorIterator<>(branchFactor, iter);
                    }

                    return iter;
                } else {
                    return new EmptyGraphTraversal();
                }
            });
        }

        public String toString() {
            return TraversalHelper.makeStepString(this, this.direction);
        }
    }

    private class EmptyGraphTraversal<S, E> extends EmptyTraversal<S, E> implements GraphTraversal<S, E> {

        public GraphTraversal<S, E> submit(final GraphComputer computer) {
            return new EmptyGraphTraversal<>();
        }
    }

    private class EdgeIterator implements Iterator<Edge> {
        private final Iterator<Edge> baseIterator;

        private Edge nextElement;

        private EdgeIterator(final Iterator<Edge> baseIterator) {
            this.baseIterator = baseIterator;
            advanceToNext();
        }

        public boolean hasNext() {
            return null != nextElement;
        }

        public Edge next() {
            if (null == nextElement) {
                throw new NoSuchElementException();
            }

            Edge tmp = nextElement;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            while (baseIterator.hasNext()) {
                Edge nextBaseElement = baseIterator.next();
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
            if (direction == Direction.BOTH) {
                throw new IllegalArgumentException();
            }

            this.direction = direction;
            this.edgeIterator = baseIterator;

            advanceToNext();
        }

        public boolean hasNext() {
            return null != nextVertex;
        }

        public Vertex next() {
            if (null == nextVertex) {
                throw new NoSuchElementException();
            }

            Vertex tmp = nextVertex;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            do {
                while (null != vertexIterator && vertexIterator.hasNext()) {
                    Vertex nextBaseVertex = vertexIterator.next();
                    if (testVertex(nextBaseVertex)) {
                        nextVertex = nextBaseVertex;
                        return;
                    }
                }

                if (!edgeIterator.hasNext()) {
                    nextVertex = null;
                    return;
                }

                Edge nextBaseEdge = edgeIterator.next();
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

    private class BranchFactorIterator<V> implements Iterator<V> {
        private final int branchFactor;
        private final Iterator<V> baseIterator;
        private long count = 0;

        private BranchFactorIterator(final int branchFactor,
                                     final Iterator<V> baseIterator) {
            this.branchFactor = branchFactor;
            this.baseIterator = baseIterator;
        }

        public boolean hasNext() {
            return count < branchFactor && baseIterator.hasNext();
        }

        public V next() {
            if (count >= branchFactor) {
                throw new NoSuchElementException();
            }

            count++;
            return baseIterator.next();
        }
    }

    private class MultiIterator<V> implements Iterator<V> {
        private final Iterator<V>[] baseIterators;
        private int iteratorIndex;
        private V nextItem;

        private MultiIterator(Iterator<V>... baseIterators) {
            this.baseIterators = baseIterators;
            if (0 == baseIterators.length) {
                throw new IllegalArgumentException("must supply at least one base iterator");
            }
            iteratorIndex = 0;
            advanceToNext();
        }

        public boolean hasNext() {
            return null != nextItem;
        }

        public V next() {
            if (null == nextItem) {
                throw new NoSuchElementException();
            }

            V tmp = nextItem;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            nextItem = null;

            do {
                if (iteratorIndex >= baseIterators.length) {
                    return;
                }

                if (baseIterators[iteratorIndex].hasNext()) {
                    nextItem = baseIterators[iteratorIndex].next();
                    return;
                }

                iteratorIndex++;
            } while (true);
        }
    }
}
