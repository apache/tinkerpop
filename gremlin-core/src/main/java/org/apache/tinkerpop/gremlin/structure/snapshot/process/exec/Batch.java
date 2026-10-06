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

import java.util.Objects;

/**
 * A columnar batch of up to {@link #capacity} entries of one {@link Lane}, the unit that operators exchange. Arrays are
 * public for speed and are indexed {@code [0, n)}; which of them exist depends on the lane, see {@link Lane}. Arrays
 * that the lane does not use are null. The batch is owned by whoever created it: an operator fills the batch its
 * consumer passed to {@code next}, and never keeps a reference to it afterwards.
 * <p/>
 * A {@code VAL} entry is either a lazy reference to a column entry ({@code key[i] >= 0} is the column id registered
 * with {@link CsrExecutionContext#registerColumn} and {@code aux[i]} the entry index) or a decoded object
 * ({@code key[i] == DECODED}, value in {@code val[i]}, which may be null). {@link Materializer#value} decodes either.
 * <p/>
 * Instances are not thread-safe.
 */
public final class Batch {

    /**
     * The default number of entries, chosen so a batch fits in L2 cache.
     */
    public static final int DEFAULT_CAPACITY = 4096;

    /**
     * The {@code key} value of a {@code VAL} entry that holds a decoded object in {@code val}.
     */
    public static final int DECODED = -1;

    public final Lane lane;
    public final int capacity;
    /**
     * Whether an {@code E} batch carries {@link #src}.
     */
    public final boolean recordSource;

    public final int[] ord;
    public final int[] key;
    public final long[] aux;
    public final int[] src;
    public final Object[] val;
    public final long[] bulk;

    /**
     * The number of entries.
     */
    public int n;

    public Batch(final Lane lane, final int capacity, final boolean recordSource) {
        this.lane = Objects.requireNonNull(lane);
        if (capacity < 1) throw new IllegalArgumentException("Batch capacity must be positive");
        if (recordSource && lane != Lane.E) throw new IllegalArgumentException("Only an E batch records sources");
        this.capacity = capacity;
        this.recordSource = recordSource;
        this.bulk = new long[capacity];
        final boolean hasOrd = lane != Lane.VAL && lane != Lane.SCALAR;
        this.ord = hasOrd ? new int[capacity] : null;
        this.key = lane == Lane.VP || lane == Lane.EP || lane == Lane.MP || lane == Lane.VAL ? new int[capacity] : null;
        this.aux = lane == Lane.VP || lane == Lane.MP || lane == Lane.VAL ? new long[capacity] : null;
        this.src = recordSource || lane == Lane.MP ? new int[capacity] : null;
        this.val = lane == Lane.VAL || lane == Lane.SCALAR ? new Object[capacity] : null;
    }

    public Batch(final Lane lane) {
        this(lane, DEFAULT_CAPACITY, false);
    }

    public Batch(final Lane lane, final int capacity) {
        this(lane, capacity, false);
    }

    /**
     * A batch of the same shape, empty.
     */
    public Batch newLike() {
        return new Batch(lane, capacity, recordSource);
    }

    public void clear() {
        n = 0;
        if (val != null) java.util.Arrays.fill(val, null);
    }

    public boolean isEmpty() {
        return n == 0;
    }

    public boolean isFull() {
        return n == capacity;
    }

    public int remaining() {
        return capacity - n;
    }

    /**
     * The sum of the bulks of all entries.
     */
    public long totalBulk() {
        long sum = 0;
        for (int i = 0; i < n; i++) sum += bulk[i];
        return sum;
    }

    /**
     * The approximate heap size of a batch with this shape, for budget accounting.
     */
    public long estimatedBytes() {
        long bytes = 8L * capacity;
        if (ord != null) bytes += 4L * capacity;
        if (key != null) bytes += 4L * capacity;
        if (aux != null) bytes += 8L * capacity;
        if (src != null) bytes += 4L * capacity;
        if (val != null) bytes += 8L * capacity;
        return bytes;
    }

    /**
     * The largest power-of-two fraction of {@code capacity}, but at least {@code 64} entries, whose batch of this shape
     * is estimated at no more than {@code maxBytes}. Operators that must hold a batch of their own use it so a small
     * budget shrinks the batch instead of failing.
     */
    public static int capacityWithin(final Lane lane, final boolean recordsSource, final int capacity,
                                     final long maxBytes) {
        final long perEntry = new Batch(lane, 1, recordsSource).estimatedBytes();
        int fit = capacity;
        while (fit > 64 && fit * perEntry > maxBytes) fit >>= 1;
        return fit;
    }

    // ---------------------------------------------------------------- appenders; the caller checks isFull()

    public void addV(final int vertex, final long entryBulk) {
        ord[n] = vertex;
        bulk[n++] = entryBulk;
    }

    public void addE(final int edge, final long entryBulk) {
        ord[n] = edge;
        bulk[n++] = entryBulk;
    }

    /**
     * Adds an edge with the vertex it was reached from; the batch must {@link #recordSource record sources}.
     */
    public void addE(final int edge, final int source, final long entryBulk) {
        ord[n] = edge;
        src[n] = source;
        bulk[n++] = entryBulk;
    }

    public void addVP(final int owner, final int keyCode, final long vertexProperty, final long entryBulk) {
        ord[n] = owner;
        key[n] = keyCode;
        aux[n] = vertexProperty;
        bulk[n++] = entryBulk;
    }

    public void addEP(final int edge, final int keyCode, final long entryBulk) {
        ord[n] = edge;
        key[n] = keyCode;
        bulk[n++] = entryBulk;
    }

    public void addMP(final int owner, final int keyCode, final int metaKeyCode, final long vertexProperty,
                      final long entryBulk) {
        ord[n] = owner;
        key[n] = keyCode;
        src[n] = metaKeyCode;
        aux[n] = vertexProperty;
        bulk[n++] = entryBulk;
    }

    /**
     * Adds a lazy reference to an entry of a registered column.
     */
    public void addColumnValue(final int columnId, final long entryIndex, final long entryBulk) {
        key[n] = columnId;
        aux[n] = entryIndex;
        bulk[n++] = entryBulk;
    }

    /**
     * Adds a decoded value, which may be null, to a {@code VAL} or {@code SCALAR} batch.
     */
    public void addValue(final Object value, final long entryBulk) {
        if (key != null) key[n] = DECODED;
        val[n] = value;
        bulk[n++] = entryBulk;
    }

    /**
     * Appends entry {@code i} of a batch of the same lane, with its bulk.
     */
    public void copyEntry(final Batch from, final int i) {
        copyEntry(from, i, from.bulk[i]);
    }

    /**
     * Appends entry {@code i} of a batch of the same lane, with a different bulk. The source is copied only when both
     * batches have it.
     */
    public void copyEntry(final Batch from, final int i, final long entryBulk) {
        if (ord != null) ord[n] = from.ord[i];
        if (key != null) key[n] = from.key[i];
        if (aux != null) aux[n] = from.aux[i];
        if (src != null && from.src != null) src[n] = from.src[i];
        if (val != null) val[n] = from.val[i];
        bulk[n++] = entryBulk;
    }
}
