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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

final class TinkerElementContainer<T extends TinkerElement> {
    private T element;
    private Object elementId;
    private boolean isDeleted = false;
    private ThreadLocal<T> transactionUpdatedValue = ThreadLocal.withInitial(() -> null);
    private ThreadLocal<Boolean> isDeletedInTx = ThreadLocal.withInitial(() -> false);

    // needed to understand whether this element is used in other transactions or it can be deleted during rollback
    private AtomicInteger usesInTransactions = new AtomicInteger(0);

    private final ReentrantLock lock = new ReentrantLock();

    public TinkerElementContainer(final Object elementId) {
        this.elementId = elementId;
    }

    public T get() {
        if (isDeletedInTx.get()) return null;
        if (transactionUpdatedValue.get() != null) return transactionUpdatedValue.get();
        if (isDeleted) return null;
        return element;
    }

    public T getUnmodified() {
        return element;
    }

    public Object getElementId() {
        return elementId;
    }

    public boolean isChanged() {
        return isDeletedInTx.get() || transactionUpdatedValue.get() != null;
    }

    public boolean isDeleted() { return isDeleted || isDeletedInTx.get(); }

    public void markDeleted() {
        isDeletedInTx.set(true);
    }

    public void touch(final T transactionElement, TinkerThreadLocalTransaction tx) {
        elementId = transactionElement.id();
        if (element != transactionElement) return;

        element = (T) transactionElement.clone();
        setDraft(transactionElement, tx);
    }

    public void setDraft(final T transactionElement, TinkerThreadLocalTransaction tx) {
        elementId = transactionElement.id();
        if (transactionUpdatedValue.get() == null && !isDeletedInTx.get())
            usesInTransactions.incrementAndGet();
        transactionUpdatedValue.set(transactionElement);
        tx.touch(this);
    }

    public boolean updatedOutsideTransaction() {
        // todo: do we need to check version on delete?
        return isDeleted ||
                element != null
                        && transactionUpdatedValue.get() != null
                        && transactionUpdatedValue.get().version() != element.version();
    }

    public void commit(final long txVersion) {
        if (isDeletedInTx.get()) {
            element.removed = true;
            element = null;
            isDeleted = true;
        } else {
            element = transactionUpdatedValue.get();
            element.currentVersion = txVersion;
            // todo: if (isDeleted) throw?
        }
        usesInTransactions.decrementAndGet();
        reset();
    }

    public void rollback() {
        reset();
        usesInTransactions.decrementAndGet();
    }

    public boolean canBeRemoved() {
        return usesInTransactions.get() == 0 && (isDeleted || element == null);
    }

    private void reset() {
        transactionUpdatedValue.set(null);
        isDeletedInTx.set(false);
    }

    public boolean tryLock() {
        return lock.tryLock();
    }

    public void releaseLock() {
        if (lock.isLocked())
            lock.unlock();
    }
}
