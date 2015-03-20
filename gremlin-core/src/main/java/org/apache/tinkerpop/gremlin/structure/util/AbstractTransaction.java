/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A simple base class for {@link Transaction} that provides some common functionality and default behavior.  Vendors
 * can use this class as a starting point for their own implementations. Implementers should note that this
 * class assumes that threaded transactions are not enabled.  Vendors should explicitly override
 * {@link #create} to implement that functionality if required.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractTransaction implements Transaction {
    protected Consumer<Transaction> readWriteConsumer;
    protected Consumer<Transaction> closeConsumer;

    private Graph g;

    public AbstractTransaction(final Graph g) {
        // auto transaction behavior
        readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

        // commit on close
        closeConsumer = CLOSE_BEHAVIOR.COMMIT;

        this.g = g;
    }

    /**
     * Called within {@link #open} if it is determined that the transaction is not yet open given {@link #isOpen}.
     * Implementers should assume the transaction is not yet started and should thus open one.
     */
    protected abstract void doOpen();

    /**
     * Called with {@link #commit} after the {@link #readWriteConsumer} has been notified.  Implementers should
     * include their commit logic here.
     */
    protected abstract void doCommit();

    /**
     * Called with {@link #rollback} after the {@link #readWriteConsumer} has been notified.  Implementers should
     * include their rollback logic here.
     */
    protected abstract void doRollback();

    @Override
    public void open() {
        if (isOpen())
            throw Transaction.Exceptions.transactionAlreadyOpen();
        else
            doOpen();
    }

    @Override
    public void commit() {
        readWriteConsumer.accept(this);
        doCommit();
    }

    @Override
    public void rollback() {
        readWriteConsumer.accept(this);
        doRollback();
    }

    @Override
    public <R> Workload<R> submit(final Function<Graph, R> work) {
        return new Workload<>(g, work);
    }

    @Override
    public <G extends Graph> G create() {
        throw Transaction.Exceptions.threadedTransactionsNotSupported();
    }

    @Override
    public void readWrite() {
        readWriteConsumer.accept(this);
    }

    @Override
    public void close() {
        closeConsumer.accept(this);
    }

    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        readWriteConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
        return this;
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        closeConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull);
        return this;
    }
}
