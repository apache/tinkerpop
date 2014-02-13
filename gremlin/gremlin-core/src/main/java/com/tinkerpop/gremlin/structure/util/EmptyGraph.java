package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.process.util.EmptyTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Strategy;
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
    public <A extends Traversal<?, Vertex>> A V() {
        return (A) EmptyTraversal.instance();
    }

    @Override
    public <A extends Traversal<?, Edge>> A E() {
        return (A) EmptyTraversal.instance();
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
    public Strategy strategy() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Annotations annotations() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() throws Exception {
        throw new IllegalStateException(MESSAGE);
    }
}