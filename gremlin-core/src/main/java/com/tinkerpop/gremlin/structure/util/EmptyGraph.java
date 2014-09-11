package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.EmptyGraphTraversal;
import com.tinkerpop.gremlin.process.util.EmptyTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyGraph implements Graph {

    private static final String MESSAGE = "The graph is immutable and empty";
    private static final EmptyGraph INSTANCE = new EmptyGraph();

    private EmptyGraph() {

    }

    public static Graph instance() {
        return INSTANCE;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return EmptyGraphTraversal.instance();
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return EmptyGraphTraversal.instance();
    }

    @Override
    public <T extends Traversal<S, S>, S> T of(final Class<T> traversal) {
        return (T) EmptyTraversal.instance();
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        return EmptyGraphTraversal.instance();
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
    public Transaction tx() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Variables variables() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() throws Exception {
        throw new IllegalStateException(MESSAGE);
    }
}