package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;

import javax.transaction.Status;
import javax.transaction.SystemException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jTransaction implements Transaction {
    private final Neo4jGraph graph;
    private Consumer<Transaction> readWriteConsumer;
    private Consumer<Transaction> closeConsumer;

    public Neo4jTransaction(final Neo4jGraph graph) {
        this.graph = graph;

        // auto transaction behavior
        this.readWriteConsumer = tx -> { if (!tx.isOpen()) tx.open(); };

        // commit on close
        this.closeConsumer = tx -> { if (tx.isOpen()) tx.commit(); };
    }

    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else
            graph.threadLocalTx.set(this.graph.getRawGraph().beginTx());
    }

    @Override
    public void commit() {
        if (!isOpen())
            return;

        try {
            // todo: why: https://groups.google.com/forum/#!msg/neo4j/w1L_21z0z04/Q-RcqeEmp14J
            //graph.threadLocalTx.get().success();
        } finally {
            graph.threadLocalTx.get().close();
            graph.threadLocalTx.remove();
        }
    }

    @Override
    public void rollback() {
        if (!isOpen())
            return;

        try {
            javax.transaction.Transaction t = this.graph.transactionManager.getTransaction();
            if (null == t || t.getStatus() == Status.STATUS_ROLLEDBACK)
                return;

            graph.threadLocalTx.get().failure();
        } catch (SystemException e) {
            throw new RuntimeException(e); // todo: generalize and make consistent
        } finally {
            graph.threadLocalTx.get().close();
            graph.threadLocalTx.remove();
        }
    }

    @Override
    public <G extends Graph, R> Workload<G, R> submit(final Function<G, R> work) {
        return new Workload<>((G) this.graph, work);
    }

    @Override
    public <G extends Graph> G create() {
        // todo: need a feature for threaded transactions
        return null;
    }

    @Override
    public boolean isOpen() {
        boolean xc = (null != graph.threadLocalTx.get());
        return xc;
    }

    @Override
    public void readWrite() {
        this.readWriteConsumer.accept(this);
    }

    @Override
    public void close() {
        this.closeConsumer.accept(this);
    }

    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        if (null == consumer)
            throw new IllegalArgumentException("consumer"); // todo: exception consistency

        this.readWriteConsumer = consumer;
        return this;
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        if (null == consumer)
            throw new IllegalArgumentException("consumer");   // todo: exception consistency

        this.closeConsumer = consumer;
        return this;
    }
}
