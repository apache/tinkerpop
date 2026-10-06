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

/**
 * The dense {@link Frontier}: a {@code long[universe]} bulk array and a bitset of the nonzero positions, so merging is
 * an array add and draining scans the bitset words. It costs {@code 8 * universe + universe / 8} bytes regardless of
 * how many ordinals are present.
 */
public final class DenseFrontier implements Frontier {

    private final MemoryBudget budget;
    private final String owner;
    private final Lane lane;
    private final int universe;
    private final long[] bulks;
    private final long[] bits;
    private long distinct;
    private int cursorWord;
    private long cursorBits;
    private boolean cursorLoaded;
    private long reserved;

    public DenseFrontier(final MemoryBudget budget, final String owner, final Lane lane, final int universe) {
        if (!lane.isElement()) throw new IllegalArgumentException("A frontier holds vertices or edges, not " + lane);
        this.budget = budget;
        this.owner = owner;
        this.lane = lane;
        this.universe = universe;
        final long bytes = bytesFor(universe);
        budget.reserve(bytes, owner);
        this.reserved = bytes;
        this.bulks = new long[universe];
        this.bits = new long[(universe + 63) >>> 6];
    }

    /**
     * The bytes a dense frontier over the universe takes.
     */
    public static long bytesFor(final int universe) {
        return 8L * universe + 8L * ((universe + 63) >>> 6);
    }

    @Override
    public Lane lane() {
        return lane;
    }

    @Override
    public boolean isDense() {
        return true;
    }

    @Override
    public void add(final int ordinal, final long bulk) {
        final int word = ordinal >>> 6;
        final long bit = 1L << (ordinal & 63);
        if ((bits[word] & bit) == 0) {
            bits[word] |= bit;
            distinct++;
        }
        bulks[ordinal] += bulk;
    }

    /**
     * The merged bulk of the ordinal, 0 if absent.
     */
    public long bulkOf(final int ordinal) {
        return (bits[ordinal >>> 6] & (1L << (ordinal & 63))) == 0 ? 0 : bulks[ordinal];
    }

    @Override
    public void seal() {
    }

    @Override
    public long distinct() {
        return distinct;
    }

    @Override
    public int drain(final Batch out) {
        if (out.lane != lane) throw new IllegalArgumentException("Batch lane " + out.lane + " is not " + lane);
        int appended = 0;
        while (!out.isFull()) {
            if (!cursorLoaded) {
                if (cursorWord >= bits.length) break;
                cursorBits = bits[cursorWord];
                cursorLoaded = true;
            }
            if (cursorBits == 0) {
                cursorWord++;
                cursorLoaded = false;
                continue;
            }
            final int low = Long.numberOfTrailingZeros(cursorBits);
            cursorBits &= cursorBits - 1;
            final int ordinal = (cursorWord << 6) + low;
            out.ord[out.n] = ordinal;
            out.bulk[out.n++] = bulks[ordinal];
            appended++;
        }
        return appended;
    }

    @Override
    public void rewind() {
        cursorWord = 0;
        cursorBits = 0;
        cursorLoaded = false;
    }

    @Override
    public void clear() {
        java.util.Arrays.fill(bulks, 0);
        java.util.Arrays.fill(bits, 0);
        distinct = 0;
        rewind();
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
}
