/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;

import java.util.Arrays;

/**
 * A bounded memo of child results per input ordinal, for deterministic children that are fed bulk 1: the results of a
 * run are kept in one store batch and found through an open-addressing table. When the store or the table is full the
 * whole memo is dropped and refilled, so memory stays fixed. Reserves its memory from the budget and is not created
 * when that would take more than a quarter of what is left.
 */
final class ResultCache implements AutoCloseable {

    private final MemoryBudget budget;
    private final String owner;
    private final long reserved;
    private final int batchSize;
    private final Batch store;
    private final int[] keys;
    private final long[] slots;
    private final int mask;
    private final int maxKeys;
    private int count;
    private int mark;

    private ResultCache(final MemoryBudget budget, final String owner, final long reserved, final int batchSize,
                        final Batch store, final int tableSize, final int maxKeys) {
        this.budget = budget;
        this.owner = owner;
        this.reserved = reserved;
        this.batchSize = batchSize;
        this.store = store;
        this.keys = new int[tableSize];
        this.slots = new long[tableSize];
        this.mask = tableSize - 1;
        this.maxKeys = maxKeys;
    }

    /**
     * @param lane the lane of the results
     * @return the cache, or null if it does not fit the budget
     */
    static ResultCache tryCreate(final CsrExecutionContext ctx, final Lane lane, final String owner) {
        final int batchSize = ctx.batchSize();
        final int capacity = (int) Math.min(Integer.MAX_VALUE / 4, 4L * batchSize);
        int table = 16;
        while (table < 2 * capacity) table <<= 1;
        final Batch store = new Batch(lane, capacity, false);
        final long bytes = store.estimatedBytes() + 12L * table;
        final MemoryBudget budget = ctx.budget();
        if (bytes > budget.available() / 4 || !budget.tryReserve(bytes, owner)) return null;
        return new ResultCache(budget, owner, bytes, batchSize, store, table, capacity);
    }

    /**
     * The batch that holds the cached results.
     */
    Batch store() {
        return store;
    }

    /**
     * @return -1 if the ordinal is not cached, otherwise {@code start << 32 | length} of its results in the store
     */
    long lookup(final int ordinal) {
        int slot = hash(ordinal);
        while (true) {
            final int key = keys[slot];
            if (key == 0) return -1;
            if (key == ordinal + 1) return slots[slot];
            slot = (slot + 1) & mask;
        }
    }

    /**
     * Starts recording the results of one ordinal; makes room first if the store could not hold a few more batches.
     */
    void beginRecord() {
        if (store.n + batchSize > store.capacity || count >= maxKeys) clear();
        mark = store.n;
    }

    /**
     * Records the entries {@code from} to {@code to} of a result batch.
     *
     * @return false if they do not fit, which abandons the recording
     */
    boolean append(final Batch results, final int from, final int to) {
        if (to - from > store.capacity - store.n) {
            abort();
            return false;
        }
        for (int j = from; j < to; j++) store.copyEntry(results, j);
        return true;
    }

    /**
     * Ends the recording that started with {@link #beginRecord()}, keeping it for the ordinal.
     */
    void commit(final int ordinal) {
        int slot = hash(ordinal);
        while (keys[slot] != 0) slot = (slot + 1) & mask;
        keys[slot] = ordinal + 1;
        slots[slot] = ((long) mark << 32) | (store.n - mark);
        count++;
    }

    /**
     * Drops the recording that started with {@link #beginRecord()}.
     */
    void abort() {
        store.n = mark;
    }

    void clear() {
        store.clear();
        Arrays.fill(keys, 0);
        count = 0;
        mark = 0;
    }

    private int hash(final int ordinal) {
        return (ordinal * 0x9E3779B9) >>> 7 & mask;
    }

    @Override
    public void close() {
        budget.release(reserved, owner);
    }
}
