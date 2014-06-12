package com.tinkerpop.gremlin.structure.util.subgraph;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubgraphEdgeToVertexTraversal extends DefaultGraphTraversal<Vertex, Vertex> {

    private final Traversal<Vertex, Edge> baseTraversal;
    private final Vertex baseVertex;
    private final Function<Edge, Boolean> edgeCriterion;
    private final Direction direction;

    private Vertex nextVertex;

    public SubgraphEdgeToVertexTraversal(final Traversal<Vertex, Edge> baseTraversal,
                                         final Vertex baseVertex,
                                         final Function<Edge, Boolean> edgeCriterion,
                                         final Direction direction) {
        this.baseTraversal = baseTraversal;
        this.baseVertex = baseVertex;
        this.edgeCriterion = edgeCriterion;
        this.direction = direction;

        advanceToNext();
    }

    @Override
    public Memory memory() {
        return baseTraversal.memory();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> submit(final TraversalEngine engine) {
        Traversal<Vertex, Edge> newBaseTraversal = baseTraversal.submit(engine);
        return new SubgraphEdgeToVertexTraversal(newBaseTraversal, baseVertex, edgeCriterion, direction);
    }

    @Override
    public boolean hasNext() {
        return null != nextVertex;
    }

    @Override
    public Vertex next() {
        if (null == nextVertex) {
            throw FastNoSuchElementException.instance();
        }

        Vertex tmp = nextVertex;
        advanceToNext();

        return tmp;
    }

    private void advanceToNext() {
        while (baseTraversal.hasNext()) {
            Edge nextBaseEdge = baseTraversal.next();

            if (edgeCriterion.apply(nextBaseEdge)) {
                switch (direction) {
                    case OUT:
                        nextVertex = nextBaseEdge.outV().next();
                        break;
                    case IN:
                        nextVertex = nextBaseEdge.inV().next();
                        break;
                    case BOTH:
                        nextVertex = nextBaseEdge.outV().next();
                        if (nextVertex.equals(baseVertex)) {
                            nextVertex = nextBaseEdge.inV().next();
                        }
                        break;
                }
                return;
            }
        }

        nextVertex = null;
    }
}
