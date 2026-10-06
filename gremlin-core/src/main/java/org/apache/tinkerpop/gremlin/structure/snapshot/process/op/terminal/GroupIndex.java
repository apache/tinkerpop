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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.Manifest;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps the key of each entry to a dense group number, assigned in first-seen order, for {@code groupCount} and
 * {@code group}. The keys that can be read without objects use a dense slot array over label codes, element ordinals
 * or a small integral range from the column statistics; everything else uses a hash map. The final key objects
 * (facades, label strings, decoded values) are only built by {@link #keyOf(int)} for the distinct groups. Every
 * allocation is reserved from the memory budget under the owner given to {@link #create}, which must not be shared.
 */
abstract class GroupIndex implements AutoCloseable {

    /**
     * The largest span of an integral column that is indexed densely.
     */
    static final long MAX_RANGE_SPAN = 1L << 20;

    private static final long HASH_GROUP_BYTES = 96;

    protected final CsrExecutionContext ctx;
    protected final String owner;
    protected int size;

    protected GroupIndex(final CsrExecutionContext ctx, final String owner) {
        this.ctx = ctx;
        this.owner = owner;
    }

    /**
     * The group of the entry, or -1 when its key is non-productive.
     */
    abstract int indexOf(Batch b, int i);

    /**
     * The key object of the group.
     */
    abstract Object keyOf(int group);

    int size() {
        return size;
    }

    abstract void reset();

    @Override
    public abstract void close();

    static GroupIndex create(final CsrExecutionContext ctx, final Keys.Key key, final Lane lane, final String owner) {
        GroupIndex dense = null;
        if (lane == Lane.V || lane == Lane.E) {
            if (key instanceof Keys.Identity) {
                dense = OrdinalGroups.tryCreate(ctx, owner, lane, null);
            } else if (key instanceof Keys.Token) {
                final T token = ((Keys.Token) key).token();
                if (token == T.label) dense = LabelGroups.tryCreate(ctx, owner, lane);
                else if (token == T.id) dense = OrdinalGroups.tryCreate(ctx, owner, lane, T.id);
            } else if (key instanceof Keys.Value) {
                dense = RangeGroups.tryCreate(ctx, owner, lane, (Keys.Value) key);
            }
        }
        return dense != null ? dense : new ObjectGroups(ctx, owner, key, lane);
    }

    // ---------------------------------------------------------------- dense variants

    /**
     * A slot array over a small universe plus the group representatives.
     */
    private abstract static class Dense extends GroupIndex {

        private final int[] slots;
        protected long[] reps = new long[16];

        Dense(final CsrExecutionContext ctx, final String owner, final int slotCount) {
            super(ctx, owner);
            this.slots = new int[slotCount];
            Arrays.fill(slots, -1);
        }

        /**
         * Reserves the slot array; false when the budget cannot hold it.
         */
        static boolean reserveSlots(final CsrExecutionContext ctx, final String owner, final long slotCount) {
            return ctx.budget().tryReserve(4L * slotCount + 8L * 16, owner);
        }

        /**
         * The slot of the entry, or -1 when its key is non-productive.
         */
        abstract int slot(Batch b, int i);

        abstract long rep(Batch b, int i);

        void created(final int slot, final int group) {
        }

        @Override
        final int indexOf(final Batch b, final int i) {
            final int slot = slot(b, i);
            if (slot < 0) return -1;
            int group = slots[slot];
            if (group < 0) {
                if (size == reps.length) {
                    ctx.budget().reserve(8L * reps.length, owner);
                    reps = Arrays.copyOf(reps, reps.length * 2);
                }
                group = size++;
                reps[group] = rep(b, i);
                slots[slot] = group;
                created(slot, group);
            }
            return group;
        }

        @Override
        void reset() {
            Arrays.fill(slots, -1);
            size = 0;
        }

        @Override
        public final void close() {
            ctx.budget().releaseAll(owner);
        }
    }

    /**
     * Elements keyed by themselves or by their id: one slot per ordinal.
     */
    private static final class OrdinalGroups extends Dense {

        private final Lane lane;
        private final T token;

        private OrdinalGroups(final CsrExecutionContext ctx, final String owner, final Lane lane, final T token) {
            super(ctx, owner, lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount());
            this.lane = lane;
            this.token = token;
        }

        static GroupIndex tryCreate(final CsrExecutionContext ctx, final String owner, final Lane lane, final T token) {
            final CsrSnapshot snapshot = ctx.snapshot();
            final long universe = lane == Lane.V ? snapshot.vertexCount() : snapshot.edgeCount();
            return reserveSlots(ctx, owner, universe) ? new OrdinalGroups(ctx, owner, lane, token) : null;
        }

        @Override
        int slot(final Batch b, final int i) {
            return b.ord[i];
        }

        @Override
        long rep(final Batch b, final int i) {
            return b.ord[i];
        }

        @Override
        Object keyOf(final int group) {
            final int ordinal = (int) reps[group];
            final org.apache.tinkerpop.gremlin.structure.Element facade = lane == Lane.V
                    ? ctx.graph().vertexAt(ordinal) : ctx.graph().edgeAt(ordinal);
            return token == null ? facade : token.apply(facade);
        }
    }

    /**
     * Elements keyed by label: one slot per label code, plus one for elements without a label.
     */
    private static final class LabelGroups extends Dense {

        private final Lane lane;
        private final int labelCount;

        private LabelGroups(final CsrExecutionContext ctx, final String owner, final Lane lane, final int labelCount) {
            super(ctx, owner, labelCount + 1);
            this.lane = lane;
            this.labelCount = labelCount;
        }

        static GroupIndex tryCreate(final CsrExecutionContext ctx, final String owner, final Lane lane) {
            final CsrSnapshot snapshot = ctx.snapshot();
            final int labels = lane == Lane.V ? snapshot.vertexLabels().size() : snapshot.edgeLabels().size();
            return reserveSlots(ctx, owner, labels + 1L) ? new LabelGroups(ctx, owner, lane, labels) : null;
        }

        @Override
        int slot(final Batch b, final int i) {
            final CsrSnapshot snapshot = ctx.snapshot();
            final int code = lane == Lane.V ? snapshot.vertexLabelCode(b.ord[i]) : snapshot.edgeLabelCode(b.ord[i]);
            return code < 0 ? labelCount : code;
        }

        @Override
        long rep(final Batch b, final int i) {
            return b.ord[i];
        }

        @Override
        Object keyOf(final int group) {
            final int ordinal = (int) reps[group];
            return lane == Lane.V ? ctx.snapshot().vertexLabel(ordinal) : ctx.snapshot().edgeLabel(ordinal);
        }
    }

    /**
     * A single-valued integral property keyed by value: one slot per value in the column's range, plus one for the
     * entries without a value when the key is productive.
     */
    private static final class RangeGroups extends Dense {

        private final Lane lane;
        private final ColumnReader column;
        private final long min;
        private final int span;
        private final boolean productive;
        private final ValueType type;
        private int nullGroup = -1;

        private RangeGroups(final CsrExecutionContext ctx, final String owner, final Lane lane,
                            final ColumnReader column, final long min, final int span, final boolean productive) {
            super(ctx, owner, span + 1);
            this.lane = lane;
            this.column = column;
            this.min = min;
            this.span = span;
            this.productive = productive;
            this.type = column.singleType();
        }

        static GroupIndex tryCreate(final CsrExecutionContext ctx, final String owner, final Lane lane,
                                    final Keys.Value key) {
            final int code = key.keyCode();
            if (code < 0) return null;
            final CsrSnapshot snapshot = ctx.snapshot();
            final ColumnReader column;
            try {
                if (lane == Lane.V) {
                    if (snapshot.isMultiProperty(code)) return null;
                    column = snapshot.vertexPropertyColumn(code);
                } else {
                    column = ctx.graph().edgeColumn(code);
                }
            } catch (UnsupportedOperationException e) {
                return null;
            }
            if (column == null || !column.isFixed() || column.singleType() == null) return null;
            final ValueType type = column.singleType();
            if (type != ValueType.BYTE && type != ValueType.SHORT && type != ValueType.INT
                    && type != ValueType.LONG) return null;
            final Manifest.ColumnInfo info = column.info();
            if (!info.hasRange() || info.nullCount() != 0 || column.hasNulls()) return null;
            final long span = info.maxValue() - info.minValue() + 1;
            if (span < 1 || span > MAX_RANGE_SPAN) return null;
            if (!reserveSlots(ctx, owner, span + 1)) return null;
            return new RangeGroups(ctx, owner, lane, column, info.minValue(), (int) span, key.productive());
        }

        @Override
        int slot(final Batch b, final int i) {
            final long entry = column.entryIndex(b.ord[i]);
            if (entry < 0) return productive ? span : -1;
            return (int) (column.rawBitsAt(entry) - min);
        }

        @Override
        long rep(final Batch b, final int i) {
            final long entry = column.entryIndex(b.ord[i]);
            return entry < 0 ? 0 : column.rawBitsAt(entry);
        }

        @Override
        void created(final int slot, final int group) {
            if (slot == span) nullGroup = group;
        }

        @Override
        Object keyOf(final int group) {
            if (group == nullGroup) return null;
            final long v = reps[group];
            switch (type) {
                case BYTE:
                    return (byte) v;
                case SHORT:
                    return (short) v;
                case INT:
                    return (int) v;
                default:
                    return v;
            }
        }

        @Override
        void reset() {
            super.reset();
            nullGroup = -1;
        }
    }

    // ---------------------------------------------------------------- hash variant

    /**
     * Any key: a hash map of the key objects.
     */
    private static final class ObjectGroups extends GroupIndex {

        private final KeyReader reader;
        private final Map<Object, Integer> index = new HashMap<>();
        private final List<Object> keys = new ArrayList<>();

        ObjectGroups(final CsrExecutionContext ctx, final String owner, final Keys.Key key, final Lane lane) {
            super(ctx, owner);
            this.reader = new KeyReader(ctx, key, lane);
        }

        @Override
        int indexOf(final Batch b, final int i) {
            final Object key = reader.read(b, i);
            if (key == KeyReader.UNPRODUCTIVE) return -1;
            Integer group = index.get(key);
            if (group == null) {
                ctx.budget().reserve(HASH_GROUP_BYTES, owner);
                group = size++;
                index.put(key, group);
                keys.add(key);
            }
            return group;
        }

        @Override
        Object keyOf(final int group) {
            return keys.get(group);
        }

        @Override
        void reset() {
            index.clear();
            keys.clear();
            size = 0;
            ctx.budget().releaseAll(owner);
            reader.reset();
        }

        @Override
        public void close() {
            reader.close();
            index.clear();
            keys.clear();
            ctx.budget().releaseAll(owner);
        }
    }
}
