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

/**
 * Container to store the value of an element which can be specific to each transaction.
 * Responsible for transactional operations for the element that stores.
 * @param <T> type of element to store.
 */
final class TinkerElementContainer<T extends TinkerElement> {
    /**
     * Committed value of element.
     */
    private T element;
    /**
     * Id of element. Used if element is removed or set to {@code null}.
     */
    private Object elementId;
    /**
     * Used to separate deleted elements from {@code null} ones.
     */
    private boolean isDeleted = false;
    /**
     * Value of elements updated in current transaction.
     */
    private final ThreadLocal<T> transactionUpdatedValue = ThreadLocal.withInitial(() -> null);
    /**
     * Marker for element deleted in current transaction.
     */
    private final ThreadLocal<Boolean> isDeletedInTx = ThreadLocal.withInitial(() -> false);

    /**
     * Marker for element modified in current transaction.
     */
    private final ThreadLocal<Boolean> isModifiedInTx = ThreadLocal.withInitial(() -> false);

    /**
     * Marker for element read in current transaction.
     */
    private final ThreadLocal<Boolean> isReadInTx = ThreadLocal.withInitial(() -> false);

    /**
     * Count of usages of container in different transactions.
     * Needed to understand whether this element is used in other transactions or it can be deleted during rollback.
     */
    private final AtomicInteger usesInTransactions = new AtomicInteger(0);

    /**
     * Used to protect container from simultaneous modification in different transactions.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor only requires the element id to be stored.
     * @param elementId id of element to store.
     */
    public TinkerElementContainer(final Object elementId) {
        this.elementId = elementId;
    }

    /**
     * Get transaction specific value of stored element.
     */
    public T get() {
        if (isDeletedInTx.get()) return null;
        if (transactionUpdatedValue.get() != null) return transactionUpdatedValue.get();
        if (isDeleted) return null;
        return element;
    }

    public T getWithClone(final TinkerTransaction tx) {
        if (isDeletedInTx.get()) return null;
        if (transactionUpdatedValue.get() != null) return transactionUpdatedValue.get();
        if (isDeleted || null == element) return null;

        final T cloned = (T) element.clone();
        transactionUpdatedValue.set(cloned);

        if (!isReadInTx.get()) {
            isReadInTx.set(true);
            usesInTransactions.incrementAndGet();
            tx.markRead(this);
        }

        return cloned;
    }

    /**
     * Get current committed value of stored element.
     */
    public T getUnmodified() {
        return element;
    }

    /**
     * Get modified in the current transaction value of stored element.
     */
    public T getModified() {
        return transactionUpdatedValue.get();
    }

    /**
     * Get id of stored element.
     */
    public Object getElementId() {
        return elementId;
    }

    /**
     * Needed to understand if the element has changed in the current transaction
     */
    public boolean isChanged() {
        return isDeletedInTx.get() || isModifiedInTx.get() && transactionUpdatedValue.get() != null;
    }

    /**
     * Used to understand if the element has been deleted in the current transaction
     */
    public boolean isDeleted() { return isDeleted || isDeletedInTx.get(); }

    /**
     * Used to understand if the element has been read in the current transaction
     */
    public boolean isRead() { return isReadInTx.get(); }

    /**
     * Mark element as deleted in the current transaction.
     */
    public void markDeleted(final TinkerTransaction tx) {
        if (!isDeletedInTx.get()) {
            usesInTransactions.incrementAndGet();
            isDeletedInTx.set(true);
            tx.markChanged(this);
        }
    }

    /**
     * Mark element as changed in the current transaction.
     * A copy of the element is made and set as a value in the transaction.
     * @param transactionElement updated element
     * @param tx current transaction
     */
    public void touch(final T transactionElement, final TinkerTransaction tx) {
        if (transactionUpdatedValue.get() == transactionElement && isModifiedInTx.get()) return;

        setDraft(transactionElement, tx);
    }

    /**
     * Set element value specific to current transaction.
     * @param transactionElement updated element
     * @param tx current transaction
     */
    public void setDraft(final T transactionElement, final TinkerTransaction tx) {
        elementId = transactionElement.id();
        if (!isModifiedInTx.get()) {
            usesInTransactions.incrementAndGet();
            isModifiedInTx.set(true);
        }
        transactionUpdatedValue.set(transactionElement);
        tx.markChanged(this);
    }

    /**
     * Used to understand if elements was changed by other transaction.
     */
    public boolean updatedOutsideTransaction() {
        // todo: do we need to check version on delete?
        final T updatedValue = transactionUpdatedValue.get();
        return isDeleted ||
                element != null && updatedValue != null && updatedValue.version() != element.version();
    }

    /**
     * Used to understand if element is in use by any transaction.
     */
    public boolean inUse() {
        return usesInTransactions.get() > 0;
    }

    /**
     * Commit changes for the stored element.
     * @param txVersion version of transaction
     */
    public void commit(final long txVersion) {
        updateUsesCount();
        if (isDeletedInTx.get()) {
            // created and deleted in same tx
            if (null != element)
                element.removed = true;
            element = null;
            isDeleted = true;
        } else if (isModifiedInTx.get()){
            element = transactionUpdatedValue.get();
            element.currentVersion = txVersion;
        }
        reset();
    }

    /**
     * Rollback changes for the stored element.
     */
    public void rollback() {
        updateUsesCount();
        reset();
    }

    /**
     * After the transaction is completed, need to reduce the usage counters to be able to delete the container.
     */
    private void updateUsesCount() {
        if (isDeletedInTx.get())
            usesInTransactions.decrementAndGet();
        if (isModifiedInTx.get())
            usesInTransactions.decrementAndGet();
        if (isReadInTx.get())
            usesInTransactions.decrementAndGet();
    }

    /**
     * Used to check if container can be removed or still used by another transaction.
     * Should be used after commit or rollback.
     */
    public boolean canBeRemoved() {
        return usesInTransactions.get() == 0 && (isDeleted || element == null);
    }

    /**
     * Cleanup changes made in the current transaction.
     */
    public void reset() {
        transactionUpdatedValue.remove();
        isDeletedInTx.set(false);
        isModifiedInTx.set(false);
        isReadInTx.set(false);
    }

    /**
     * Try to lock container to apply changes to stored element.
     * @return True if lock was successful.
     */
    public boolean tryLock() {
        return lock.tryLock();
    }

    /**
     * Release lock after applying changes.
     */
    public void releaseLock() {
        if (lock.isHeldByCurrentThread())
            lock.unlock();
    }
}
