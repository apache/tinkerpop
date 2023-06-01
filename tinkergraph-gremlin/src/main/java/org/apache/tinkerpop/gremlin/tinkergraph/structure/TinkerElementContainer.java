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

import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

final class TinkerElementContainer<T extends TinkerElement> {
    private T element;
    private ThreadLocal<T> transactionUpdatedValue = ThreadLocal.withInitial(() -> null);
    private ThreadLocal<Boolean> isDeleted = ThreadLocal.withInitial(() -> false);

    // needed to understand whether this element is used in other transactions or it can be deleted during rollback
    private AtomicInteger usesInTransactions = new AtomicInteger(0);

    private final ReentrantLock lock = new ReentrantLock();

    public void TinkerElementContainer(final T element) {
        this.element = element;
    }

    public T get() {
        if (isDeleted.get()) return null;
        if (transactionUpdatedValue.get() != null) return transactionUpdatedValue.get();
        return element;
    }

    public boolean isChanged() {
        return isDeleted.get() || transactionUpdatedValue.get() != null;
    }

    public boolean isDeleted() { return isDeleted.get(); }

    public void touch(final T transactionElement) {
        if (element != transactionElement) return;

        // todo: handle deleted
        element = (T) transactionElement.clone();
        setDraft(transactionElement);
    }

    public void markDeleted() {
        isDeleted.set(true);
    }

    public void setDraft(final T transactionElement) {
        if (transactionUpdatedValue.get() != null || isDeleted.get())
            usesInTransactions.incrementAndGet();
        transactionUpdatedValue.set(transactionElement);
    }

    public boolean updatedOutsideTransaction() {
        // todo: do we need to check version on delete?
        return element != null
                && transactionUpdatedValue.get() != null
                && transactionUpdatedValue.get().version() != element.version();
    }

    public void commit(final long txVersion) {
        if (isDeleted.get()) {
            // remove relation between edge and vertex
            if (element instanceof TinkerEdge) {
                final TinkerEdge edge = (TinkerEdge) element;
                final TinkerVertex outVertex = (TinkerVertex) edge.outVertex;
                final TinkerVertex inVertex = (TinkerVertex) edge.inVertex;

                if (null != outVertex && null != outVertex.outEdges) {
                    final Set<Edge> edges = outVertex.outEdges.get(edge.label());
                    if (null != edges)
                        edges.remove(this);
                }
                if (null != inVertex && null != inVertex.inEdges) {
                    final Set<Edge> edges = inVertex.inEdges.get(edge.label());
                    if (null != edges)
                        edges.remove(this);
                }
            }

            element = null;
        } else {
            element = transactionUpdatedValue.get();
            element.currentVersion = txVersion;
        }
        usesInTransactions.decrementAndGet();
        reset();
    }

    public Integer rollback() {
        reset();
        return usesInTransactions.decrementAndGet();
    }

    private void reset() {
        transactionUpdatedValue.set(null);
        isDeleted.set(false);
    }

    public boolean tryLock() {
        return lock.tryLock();
    }

    public void releaseLock() {
        if (lock.isLocked())
            lock.unlock();
    }
}
