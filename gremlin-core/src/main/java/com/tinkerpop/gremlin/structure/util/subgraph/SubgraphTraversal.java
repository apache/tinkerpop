package com.tinkerpop.gremlin.structure.util.subgraph;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubgraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    private final Traversal<S, E> baseTraversal;
    private final Function<Vertex, Boolean> vertexCriterion;
    private final Function<Edge, Boolean> edgeCriterion;
    private final boolean isVertexIterator;

    private Element nextElement;

    public SubgraphTraversal(final Traversal<S, E> baseTraversal,
                             final Function<Vertex, Boolean> vertexCriterion,
                             final Function<Edge, Boolean> edgeCriterion,
                             final boolean isVertexIterator) {
        this.baseTraversal = baseTraversal;
        this.vertexCriterion = vertexCriterion;
        this.edgeCriterion = edgeCriterion;
        this.isVertexIterator = isVertexIterator;

        advanceToNext();
    }

    @Override
    public Memory memory() {
        return baseTraversal.memory();
    }

    @Override
    public GraphTraversal<S, E> submit(final TraversalEngine engine) {
        Traversal<S, E> newBaseTraversal = baseTraversal.submit(engine);
        return new SubgraphTraversal(newBaseTraversal, vertexCriterion, edgeCriterion, isVertexIterator);
    }

    @Override
    public boolean hasNext() {
        return null != nextElement;
    }

    @Override
    public E next() {
        if (null == nextElement) {
            throw new NoSuchElementException();
        }

        Element tmp = nextElement;
        advanceToNext();
        return (E) tmp;
    }

    private void advanceToNext() {
        while (baseTraversal.hasNext()) {
            E nextBaseElement = baseTraversal.next();
            if (isVertexIterator) {
                if (vertexCriterion.apply((Vertex) nextBaseElement)) {
                    nextElement = new SubgraphVertex((Vertex) nextBaseElement, vertexCriterion, edgeCriterion);
                    return;
                }
            } else {
                if (edgeCriterion.apply((Edge) nextBaseElement)) {
                    Edge nextBaseEdge = (Edge) nextBaseElement;

                    // the edge must pass the edge criterion, and both of its incident vertices must also pass the vertex criterion
                    if (vertexCriterion.apply(nextBaseEdge.inV().next()) && vertexCriterion.apply(nextBaseEdge.outV().next())) {
                        nextElement = new SubgraphEdge(nextBaseEdge, vertexCriterion, edgeCriterion);
                        return;
                    }
                }
            }
        }

        nextElement = null;
    }
}
