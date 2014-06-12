package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.graph.map.VertexStep;
import com.tinkerpop.gremlin.process.util.EmptyTraversal;
import com.tinkerpop.gremlin.process.util.Reversible;
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
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubGraphGraphStrategy implements GraphStrategy {

    private final Predicate<Vertex> vertexPredicate;
    private final Predicate<Edge> edgePredicate;

    public SubGraphGraphStrategy(final Predicate<Vertex> vertexPredicate, final Predicate<Edge> edgePredicate) {
        this.vertexPredicate = vertexPredicate;
        this.edgePredicate = edgePredicate;
    }

    @Override
    public GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
        traversal.strategies().register(new SubgraphGraphTraversalStrategy());
        return traversal;
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (id) -> {
            final Vertex v = f.apply(id);
            if (!testVertex(v)) {
                throw Graph.Exceptions.elementNotFound();
            }

            return v;
        };
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (id) -> {
            final Edge e = f.apply(id);

            if (!testEdge(e)) {
                throw Graph.Exceptions.elementNotFound();
            }

            return e;
        };
    }

    private boolean testVertex(final Vertex vertex) {
        return vertexPredicate.test(vertex);
    }

    private boolean testEdge(final Edge edge) {
        // the edge must pass the edge predicate, and both of its incident vertices must also pass the vertex predicate
        return edgePredicate.test(edge) && edge.inV().hasNext() && edge.outV().hasNext();
    }

    private boolean testElement(final Element element) {
        return element instanceof Vertex
                ? testVertex((Vertex) element)
                : testEdge((Edge) element);
    }

    /*
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(o.toArray());
        };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (label, v, keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            o.addAll(Arrays.asList(this.partitionKey, writePartition));
            return f.apply(label, v, o.toArray());
        };
    }*/

    @Override
    public String toString() {
        return SubGraphGraphStrategy.class.getSimpleName();
    }

    public class SubgraphGraphTraversalStrategy implements TraversalStrategy.FinalTraversalStrategy {

        public void apply(final Traversal traversal) {
            // inject a SubgraphFilterStep after each GraphStep, VertexStep or EdgeVertexStep
            final List<Class> stepsToLookFor = Arrays.<Class>asList(GraphStep.class, VertexStep.class, EdgeVertexStep.class);
            final List<Integer> positions = new ArrayList<>();
            final List<?> traversalSteps = traversal.getSteps();
            for (int ix = 0; ix < traversalSteps.size(); ix++) {
                final int pos = ix;
                if (stepsToLookFor.stream().anyMatch(c -> c.isAssignableFrom(traversalSteps.get(pos).getClass())))
                    positions.add(ix);
            }

            Collections.reverse(positions);
            for (int pos : positions) {
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

    private class SubgraphEdgeVertexStep extends FlatMapStep<Edge, Vertex> implements Reversible {

        public Direction direction;

        public SubgraphEdgeVertexStep(final EdgeVertexStep other) {
            this(other.getTraversal(), other.getDirection());
        }

        public SubgraphEdgeVertexStep(final Traversal traversal, final Direction direction) {
            super(traversal);
            this.direction = direction;
            this.setFunction(traverser -> {
                Edge nextEdge = traverser.get();
                if (testEdge(nextEdge)) {
                    if (this.direction.equals(Direction.IN))
                        return new VertexIterator(nextEdge.inV());
                    else if (this.direction.equals(Direction.OUT))
                        return new VertexIterator(nextEdge.outV());
                    else
                        return new VertexIterator(nextEdge.bothV());
                } else {
                    return new EmptyGraphTraversal();
                }
            });
        }

        public String toString() {
            return TraversalHelper.makeStepString(this, this.direction);
        }

        public void reverse() {
            this.direction = this.direction.opposite();
        }
    }

    private class EmptyGraphTraversal<S, E> extends EmptyTraversal<S, E> implements GraphTraversal<S, E> {
    }

    private class VertexIterator implements Iterator<Vertex> {
        private final Iterator<Vertex> baseIterator;

        private Vertex nextElement;

        private VertexIterator(Iterator<Vertex> baseIterator) {
            this.baseIterator = baseIterator;
            advanceToNext();
        }

        public boolean hasNext() {
            return null != nextElement;
        }

        public Vertex next() {
            if (null == nextElement) {
                throw new NoSuchElementException();
            }

            Vertex tmp = nextElement;
            advanceToNext();
            return tmp;
        }

        private void advanceToNext() {
            while (baseIterator.hasNext()) {
                Vertex nextBaseElement = baseIterator.next();
                if (testVertex(nextBaseElement)) {
                    nextElement = nextBaseElement;
                    return;
                }
            }

            nextElement = null;
        }
    }
}