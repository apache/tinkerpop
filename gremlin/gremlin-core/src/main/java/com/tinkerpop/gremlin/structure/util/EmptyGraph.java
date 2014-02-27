package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
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

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        return (GraphTraversal) EmptyTraversal.instance();
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        return (GraphTraversal) EmptyTraversal.instance();
    }

    @Override
    public <T extends Traversal> T traversal(final Class<T> traversal) {
        return (T) EmptyTraversal.instance();
    }

    public static Graph instance() {
        return INSTANCE;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public GraphComputer compute() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Transaction tx() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Annotations annotations() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public <M extends Memory> M memory() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() throws Exception {
        throw new IllegalStateException(MESSAGE);
    }
}