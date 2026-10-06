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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;

import java.util.Arrays;

/**
 * A memo of a boolean per vertex or edge ordinal: two bitsets, one for "computed" and one for the outcome. Reserves its
 * memory from the budget; not created when that would take more than a quarter of what is left.
 */
final class OrdinalBits implements AutoCloseable {

    private final MemoryBudget budget;
    private final String owner;
    private final long reserved;
    private final long[] known;
    private final long[] value;

    private OrdinalBits(final MemoryBudget budget, final String owner, final int universe, final long bytes) {
        this.budget = budget;
        this.owner = owner;
        this.reserved = bytes;
        final int words = (int) ((universe + 63L) >>> 6);
        this.known = new long[words];
        this.value = new long[words];
    }

    /**
     * @return the memo, or null if the universe is empty or the memo does not fit the budget
     */
    static OrdinalBits tryCreate(final CsrExecutionContext ctx, final int universe, final String owner) {
        if (universe <= 0) return null;
        final long bytes = 16L * ((universe + 63L) >>> 6);
        final MemoryBudget budget = ctx.budget();
        if (bytes > budget.available() / 4 || !budget.tryReserve(bytes, owner)) return null;
        return new OrdinalBits(budget, owner, universe, bytes);
    }

    /**
     * @return 1 or 0 if the outcome is known, -1 otherwise
     */
    int get(final int ordinal) {
        final int word = ordinal >>> 6;
        final long bit = 1L << ordinal;
        if ((known[word] & bit) == 0) return -1;
        return (value[word] & bit) != 0 ? 1 : 0;
    }

    void put(final int ordinal, final boolean outcome) {
        final int word = ordinal >>> 6;
        final long bit = 1L << ordinal;
        known[word] |= bit;
        if (outcome) value[word] |= bit;
        else value[word] &= ~bit;
    }

    void clear() {
        Arrays.fill(known, 0L);
        Arrays.fill(value, 0L);
    }

    @Override
    public void close() {
        budget.release(reserved, owner);
    }
}
