package com.tinkerpop.gremlin;

import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Transaction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.query.util.DefaultGraphQuery;

import java.util.Collections;

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
    public Vertex addVertex(Object... keyValues) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery() {
            @Override
            public Iterable<Edge> edges() {
                return Collections.emptyList();
            }

            @Override
            public Iterable<Vertex> vertices() {
                return Collections.emptyList();
            }
        };
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
