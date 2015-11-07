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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A base implementation of {@link Transaction} that provides core functionality for transaction listeners using
 * {@link ThreadLocal}.  In this implementation, the listeners are bound to the current thread of execution (usually
 * the same as the transaction for most graph database implementations).  Therefore, when {@link #commit()} is
 * called on a particular thread, the only listeners that get notified are those bound to that thread.
 *
 * @see AbstractThreadedTransaction
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractThreadLocalTransaction extends AbstractTransaction {
    protected final ThreadLocal<Consumer<Transaction>> readWriteConsumerInternal =
        new ThreadLocal<Consumer<Transaction>>() {
            @Override protected Consumer<Transaction> initialValue() {
                return READ_WRITE_BEHAVIOR.AUTO;
            }
        };
    
    protected final ThreadLocal<Consumer<Transaction>> closeConsumerInternal =
        new ThreadLocal<Consumer<Transaction>>() {
            @Override protected Consumer<Transaction> initialValue() {
                return CLOSE_BEHAVIOR.ROLLBACK;
            }
        };
    
    protected final ThreadLocal<List<Consumer<Transaction.Status>>> transactionListeners = new ThreadLocal<List<Consumer<Transaction.Status>>>() {
        @Override
        protected List<Consumer<Transaction.Status>> initialValue() {
            return new ArrayList<>();
        }
    };

    public AbstractThreadLocalTransaction(final Graph g) {
        super(g);
    }

    @Override
    protected void fireOnCommit() {
        transactionListeners.get().forEach(c -> c.accept(Status.COMMIT));
    }

    @Override
    protected void fireOnRollback() {
        transactionListeners.get().forEach(c -> c.accept(Status.ROLLBACK));
    }

    @Override
    public void addTransactionListener(final Consumer<Status> listener) {
        transactionListeners.get().add(listener);
    }

    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
        transactionListeners.get().remove(listener);
    }

    @Override
    public void clearTransactionListeners() {
        transactionListeners.get().clear();
    }
    
    @Override
    protected void doReadWrite() {
        readWriteConsumerInternal.get().accept(this);
    }
    
    @Override
    protected void doClose() {
        closeConsumerInternal.get().accept(this);
        closeConsumerInternal.remove();
        readWriteConsumerInternal.remove();
    }
    
    @Override
    public Transaction onReadWrite(final Consumer<Transaction> consumer) {
        readWriteConsumerInternal.set(Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull));
        return this;
    }
    
    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        closeConsumerInternal.set(Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull));
        return this;
    }
}
