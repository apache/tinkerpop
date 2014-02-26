package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraph implements Graph {

    public GiraphGraphComputer compute() {
        return null;
    }

    public Vertex addVertex(final Object... keyValues) {
        return null;
    }

    public Traversal<Vertex, Vertex> V() {
        return null;
    }

    public Traversal<Edge, Edge> E() {
        return null;
    }

    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    public Annotations annotations() {
        return null;
    }

    public <M extends Memory> M memory() {
        return null;
    }

    public void close() {

    }

}
