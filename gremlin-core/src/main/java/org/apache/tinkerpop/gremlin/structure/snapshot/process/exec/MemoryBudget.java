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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Accounts for the heap that stateful native operators hold, against a fixed limit. An operator reserves before it
 * allocates and releases when it drops the state. The budget does not allocate anything itself and does not measure
 * the heap: it only enforces the sum of what operators declare, so declarations must be honest estimates. One budget
 * belongs to one execution and one thread.
 * <p/>
 * The owner string names the operator and its state, for example {@code "Dedup#3 hash"}, and is reported by
 * {@link CsrMemoryBudgetException} and in profile annotations.
 */
public final class MemoryBudget {

    private final long limit;
    private long reserved;
    private long peak;
    private final Map<String, Long> byOwner = new LinkedHashMap<>();

    public MemoryBudget(final long limitBytes) {
        if (limitBytes < 0) throw new IllegalArgumentException("The memory budget must not be negative");
        this.limit = limitBytes;
    }

    /**
     * A budget that never refuses, for tests and for executions with no configured limit.
     */
    public static MemoryBudget unlimited() {
        return new MemoryBudget(Long.MAX_VALUE);
    }

    /**
     * Reserves bytes for the owner.
     *
     * @throws CsrMemoryBudgetException if the budget cannot cover the request
     */
    public void reserve(final long bytes, final String owner) {
        if (!tryReserve(bytes, owner)) {
            final StringBuilder largest = new StringBuilder();
            byOwner.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(4)
                    .forEach(e -> largest.append(largest.length() == 0 ? "" : ", ").append(e.getKey()).append(" (")
                            .append(e.getValue()).append(" bytes)"));
            throw new CsrMemoryBudgetException(owner, bytes, byOwner.getOrDefault(owner, 0L), reserved, limit, largest.toString());
        }
    }

    /**
     * Reserves bytes for the owner if the budget can cover them.
     *
     * @return false, having changed nothing, if it cannot
     */
    public boolean tryReserve(final long bytes, final String owner) {
        if (bytes < 0) throw new IllegalArgumentException("Cannot reserve a negative number of bytes");
        if (bytes > limit - reserved) return false;
        reserved += bytes;
        if (reserved > peak) peak = reserved;
        byOwner.merge(owner, bytes, Long::sum);
        return true;
    }

    /**
     * Reserves bytes for the owner if the budget can cover them and the owner would hold no more than {@code quota}
     * bytes afterwards. The quota is soft and per owner string: it is how a spillable operator finds out that its state
     * has used its share and must spill. Reserving with {@link #reserve} or {@link #tryReserve} ignores quotas, which is
     * what state that cannot be spilled (results) does.
     *
     * @return false, having changed nothing, if either limit would be exceeded
     */
    public boolean tryReserveWithin(final long bytes, final String owner, final long quota) {
        if (bytes < 0) throw new IllegalArgumentException("Cannot reserve a negative number of bytes");
        if (bytes > quota - byOwner.getOrDefault(owner, 0L)) return false;
        return tryReserve(bytes, owner);
    }

    /**
     * How many bytes {@link #tryReserveWithin} would grant the owner now: the smaller of the budget's free bytes and
     * what is left of the quota, never negative.
     */
    public long roomWithin(final String owner, final long quota) {
        return Math.max(0L, Math.min(available(), quota - byOwner.getOrDefault(owner, 0L)));
    }

    /**
     * Releases bytes the owner reserved. Releasing more than the owner holds releases what it holds.
     */
    public void release(final long bytes, final String owner) {
        if (bytes < 0) throw new IllegalArgumentException("Cannot release a negative number of bytes");
        final long held = byOwner.getOrDefault(owner, 0L);
        final long freed = Math.min(bytes, held);
        reserved -= freed;
        if (freed == held) byOwner.remove(owner);
        else byOwner.put(owner, held - freed);
    }

    /**
     * Releases everything the owner holds.
     */
    public void releaseAll(final String owner) {
        release(byOwner.getOrDefault(owner, 0L), owner);
    }

    /**
     * The limit in bytes.
     */
    public long limit() {
        return limit;
    }

    /**
     * The bytes reserved now.
     */
    public long reserved() {
        return reserved;
    }

    /**
     * The most bytes that were reserved at once.
     */
    public long peak() {
        return peak;
    }

    /**
     * The bytes that can still be reserved.
     */
    public long available() {
        return limit - reserved;
    }

    /**
     * The bytes the owner holds.
     */
    public long reserved(final String owner) {
        return byOwner.getOrDefault(owner, 0L);
    }

    /**
     * A snapshot of the current reservations by owner.
     */
    public Map<String, Long> reservations() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(byOwner));
    }

    @Override
    public String toString() {
        return "MemoryBudget[" + reserved + "/" + limit + " bytes, peak " + peak + "]";
    }
}
