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

import java.util.Arrays;

/**
 * The sparse {@link Frontier}: appended (ordinal, bulk) pairs that {@link #seal()} sorts by ordinal and merges. Memory
 * is proportional to the number of {@code add} calls, 12 bytes each plus the 20 bytes of temporaries that
 * {@link #seal()} allocates, and grows by doubling with a reservation of the full 32 bytes per entry for every growth
 * step. A caller that adds more entries than the budget allows gets a {@link CsrMemoryBudgetException}; the
 * dense-to-sparse and spill policies belong to the operator that owns the frontier.
 * <p/>
 * A frontier built with a quota is capped instead: {@link #tryAdd} returns false when growing would take the owner past
 * the quota, and the caller seals, drains and {@link #clear()}s it before adding on.
 */
public final class SparseFrontier implements Frontier {

    private static final int INITIAL = 256;
    private static final int CAPPED_MINIMUM = 16;
    // 12 bytes of ordinal and bulk, plus the temporaries of seal(): order (8), merged ordinals (4), merged bulks (8)
    private static final long ENTRY_BYTES = 32L;

    private final MemoryBudget budget;
    private final String owner;
    private final Lane lane;
    private final int universe;
    private int[] ords;
    private long[] bulks;
    private int size;
    private boolean sealed = true;
    private int cursor;
    private long reserved;
    private final long quota;
    private final int minCapacity;

    public SparseFrontier(final MemoryBudget budget, final String owner, final Lane lane, final int universe) {
        this(budget, owner, lane, universe, -1L);
    }

    /**
     * Creates a frontier whose growth is capped by {@code quota} bytes for the owner, see {@link #tryAdd}. A negative
     * quota means no cap.
     */
    public SparseFrontier(final MemoryBudget budget, final String owner, final Lane lane, final int universe,
                          final long quota) {
        if (!lane.isElement()) throw new IllegalArgumentException("A frontier holds vertices or edges, not " + lane);
        this.budget = budget;
        this.owner = owner;
        this.lane = lane;
        this.universe = universe;
        this.quota = quota;
        int capacity = INITIAL;
        if (quota >= 0) {
            final long room = budget.roomWithin(owner, quota);
            while (capacity > CAPPED_MINIMUM && ENTRY_BYTES * capacity > room) capacity >>= 1;
        }
        this.minCapacity = capacity;
        reserveTo(capacity);
        this.ords = new int[capacity];
        this.bulks = new long[capacity];
    }

    // reserves for a capacity of n entries, including the sort buffer of seal()
    private void reserveTo(final int capacity) {
        final long bytes = ENTRY_BYTES * capacity;
        if (bytes > reserved) {
            budget.reserve(bytes - reserved, owner);
            reserved = bytes;
        }
    }

    @Override
    public Lane lane() {
        return lane;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public void add(final int ordinal, final long bulk) {
        if (size == ords.length) {
            final int capacity = Math.toIntExact(Math.min(Integer.MAX_VALUE - 8L, 2L * ords.length));
            reserveTo(capacity);
            grow(capacity);
        }
        ords[size] = ordinal;
        bulks[size++] = bulk;
        sealed = false;
    }

    /**
     * Adds bulk to the ordinal unless the buffer is full and cannot grow within the quota (or the budget), in which case
     * nothing is added and false is returned. Without a quota this behaves like {@link #add} and never returns false
     * but throws when the budget is exhausted.
     */
    public boolean tryAdd(final int ordinal, final long bulk) {
        if (size == ords.length) {
            if (quota < 0) {
                add(ordinal, bulk);
                return true;
            }
            if (ords.length >= Integer.MAX_VALUE - 8) return false;
            final int capacity = Math.toIntExact(Math.min(Integer.MAX_VALUE - 8L, 2L * ords.length));
            final long bytes = ENTRY_BYTES * capacity;
            if (bytes > reserved) {
                if (!budget.tryReserveWithin(bytes - reserved, owner, quota)) return false;
                reserved = bytes;
            }
            grow(capacity);
        }
        ords[size] = ordinal;
        bulks[size++] = bulk;
        sealed = false;
        return true;
    }

    private void grow(final int capacity) {
        ords = Arrays.copyOf(ords, capacity);
        bulks = Arrays.copyOf(bulks, capacity);
    }

    @Override
    public void seal() {
        if (sealed) return;
        final long[] order = new long[size];
        for (int i = 0; i < size; i++) order[i] = ((long) ords[i] << 32) | i;
        Arrays.sort(order);
        final int[] mergedOrds = new int[size];
        final long[] mergedBulks = new long[size];
        int m = 0;
        for (int i = 0; i < size; i++) {
            final int ordinal = (int) (order[i] >>> 32);
            final long bulk = bulks[(int) order[i]];
            if (m > 0 && mergedOrds[m - 1] == ordinal) {
                mergedBulks[m - 1] += bulk;
            } else {
                mergedOrds[m] = ordinal;
                mergedBulks[m++] = bulk;
            }
        }
        ords = mergedOrds.length >= minCapacity ? mergedOrds : Arrays.copyOf(mergedOrds, minCapacity);
        bulks = mergedBulks.length >= minCapacity ? mergedBulks : Arrays.copyOf(mergedBulks, minCapacity);
        size = m;
        sealed = true;
        cursor = 0;
    }

    @Override
    public long distinct() {
        return size;
    }

    @Override
    public int drain(final Batch out) {
        if (out.lane != lane) throw new IllegalArgumentException("Batch lane " + out.lane + " is not " + lane);
        seal();
        int appended = 0;
        while (!out.isFull() && cursor < size) {
            out.ord[out.n] = ords[cursor];
            out.bulk[out.n++] = bulks[cursor++];
            appended++;
        }
        return appended;
    }

    @Override
    public void rewind() {
        cursor = 0;
    }

    @Override
    public void clear() {
        size = 0;
        cursor = 0;
        sealed = true;
    }

    @Override
    public long reservedBytes() {
        return reserved;
    }

    @Override
    public void release() {
        budget.release(reserved, owner);
        reserved = 0;
    }

    /**
     * The number of vertices or edges the ordinals come from.
     */
    public int universe() {
        return universe;
    }
}
