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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * The accumulators of the reducing terminals {@code Count}, {@code Fold}, {@code Sum}, {@code Min}, {@code Max} and
 * {@code Mean}, shared by the terminal operators and by the per-group reducers of {@code group()}. Each accumulator
 * follows the arithmetic of the standard step, in emission order: the sum and the mean use {@link NumberHelper}, the
 * extremes use {@link Operator#min} and {@link Operator#max}, and an overflow surfaces as the same
 * {@link ArithmeticException}.
 */
public final class Reducers {

    private Reducers() {
    }

    /**
     * A running reduction over the entries it is given.
     */
    public interface Reducer {

        void add(Batch b, int i);

        default void addAll(final Batch b) {
            for (int i = 0; i < b.n; i++) add(b, i);
        }

        /**
         * Whether the reduction emits a value; false for the extremes, sum and mean over no entries.
         */
        boolean hasResult();

        Object result();
    }

    /**
     * A reduction whose running state can be written out and combined with the state of another reduction of the same
     * kind, so that a group operator can spill the per-group states and merge them later. The state is an array of
     * plain objects (booleans, numbers, values) that a spilling operator can encode; {@link #combine(Object[])} has the
     * semantics of having added the other reduction's entries after this one's, with the same {@link NumberHelper}
     * arithmetic that {@link #add(Batch, int)} uses.
     */
    public interface Partial extends Reducer {

        /**
         * The running state; it changes with further adds.
         */
        Object[] state();

        /**
         * Folds in a state returned by {@link #state()} of a reduction of the same kind.
         */
        void combine(Object[] state);

        /**
         * A rough size of the state in memory, for budget accounting.
         */
        long estimatedBytes();
    }

    /**
     * Whether the terminal reduces to a small state that is a {@link Partial}: count, sum, min, max and mean. Fold is not
     * one, its state is its entries.
     */
    public static boolean isPartial(final CsrOp node) {
        return node instanceof Terminals.Count || node instanceof Terminals.Sum || node instanceof Terminals.Min
                || node instanceof Terminals.Max || node instanceof Terminals.Mean;
    }

    /**
     * The partial reduction of a terminal for which {@link #isPartial(CsrOp)} holds.
     */
    public static Partial createPartial(final CsrOp node, final CsrExecutionContext ctx) {
        if (node instanceof Terminals.Count) return new CountReducer();
        if (node instanceof Terminals.Sum) return new SumReducer(ctx);
        if (node instanceof Terminals.Min) return new ExtremeReducer(ctx, true);
        if (node instanceof Terminals.Max) return new ExtremeReducer(ctx, false);
        if (node instanceof Terminals.Mean) return new MeanReducer(ctx);
        throw new IllegalArgumentException(node.name() + " does not reduce to a partial state");
    }

    static boolean isReducer(final CsrOp node) {
        return node instanceof Terminals.Count || node instanceof Terminals.Fold || node instanceof Terminals.Sum
                || node instanceof Terminals.Min || node instanceof Terminals.Max || node instanceof Terminals.Mean;
    }

    /**
     * The reducer of a reducing terminal. The entries' memory is reserved from the context's budget under the owner.
     */
    static Reducer create(final CsrOp node, final CsrExecutionContext ctx, final String owner) {
        if (node instanceof Terminals.Count) return new CountReducer();
        if (node instanceof Terminals.Fold) return new FoldReducer(ctx, owner);
        if (node instanceof Terminals.Sum) return new SumReducer(ctx);
        if (node instanceof Terminals.Min) return new ExtremeReducer(ctx, true);
        if (node instanceof Terminals.Max) return new ExtremeReducer(ctx, false);
        if (node instanceof Terminals.Mean) return new MeanReducer(ctx);
        throw new IllegalArgumentException(node.name() + " is not a reducing terminal");
    }

    private static final class CountReducer implements Partial {

        private long count;

        @Override
        public void add(final Batch b, final int i) {
            count = Math.addExact(count, b.bulk[i]);
        }

        @Override
        public void addAll(final Batch b) {
            for (int i = 0; i < b.n; i++) count = Math.addExact(count, b.bulk[i]);
        }

        @Override
        public boolean hasResult() {
            return true;
        }

        @Override
        public Object result() {
            return count;
        }

        @Override
        public Object[] state() {
            return new Object[]{count};
        }

        @Override
        public void combine(final Object[] state) {
            count = Math.addExact(count, (Long) state[0]);
        }

        @Override
        public long estimatedBytes() {
            return 32;
        }
    }

    private static final class FoldReducer implements Reducer {

        private static final long FACADE_BYTES = 48;

        private final CsrExecutionContext ctx;
        private final String owner;
        private final List<Object> list = new ArrayList<>();

        FoldReducer(final CsrExecutionContext ctx, final String owner) {
            this.ctx = ctx;
            this.owner = owner;
        }

        @Override
        public void add(final Batch b, final int i) {
            final long bulk = b.bulk[i];
            ctx.budget().reserve(8L * bulk + (b.lane.isFacade() ? FACADE_BYTES : 0), owner);
            final Object o = Materializer.materialize(ctx, b, i);
            for (long k = 0; k < bulk; k++) list.add(o);
        }

        @Override
        public boolean hasResult() {
            return true;
        }

        @Override
        public Object result() {
            return list;
        }
    }

    /**
     * The entry's column, when its value can be read as raw bits: a fixed-width integral value of a column reference.
     * Returns null for decoded values, nulls and every other type.
     */
    private static ColumnReader integralColumn(final CsrExecutionContext ctx, final Batch b, final int i) {
        if (b.lane != Lane.VAL || b.key[i] == Batch.DECODED) return null;
        final ColumnReader column = ctx.column(b.key[i]);
        if (!column.isFixed() || column.hasNulls()) return null;
        final ValueType type = column.singleType();
        return type == ValueType.BYTE || type == ValueType.SHORT || type == ValueType.INT || type == ValueType.LONG
                ? column : null;
    }

    private static Number box(final ValueType type, final long v) {
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

    private static final class SumReducer implements Partial {

        private final CsrExecutionContext ctx;
        private boolean seen;
        private Number acc;
        // the running sum while every entry so far was a LONG column reference
        private boolean fast;
        private long fastSum;

        SumReducer(final CsrExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void add(final Batch b, final int i) {
            seen = true;
            final ColumnReader column = integralColumn(ctx, b, i);
            if (column != null && column.singleType() == ValueType.LONG && (fast || acc == null || acc instanceof Long)) {
                final long product = Math.multiplyExact(column.rawBitsAt(b.aux[i]), b.bulk[i]);
                if (fast) {
                    fastSum = Math.addExact(fastSum, product);
                } else {
                    fastSum = acc == null ? product : Math.addExact(acc.longValue(), product);
                    acc = null;
                    fast = true;
                }
                return;
            }
            flush();
            final Number value = (Number) Materializer.value(ctx, b, i);
            final Class<? extends Number> clazz = null == value ? Long.class : value.getClass();
            final Number projected = NumberHelper.mul(value, NumberHelper.coerceTo(b.bulk[i], clazz));
            acc = acc == null ? projected : NumberHelper.add(acc, projected);
        }

        private void flush() {
            if (fast) {
                acc = fastSum;
                fast = false;
            }
        }

        @Override
        public boolean hasResult() {
            return seen;
        }

        @Override
        public Object result() {
            flush();
            return acc;
        }

        @Override
        public Object[] state() {
            flush();
            return new Object[]{seen, acc};
        }

        @Override
        public void combine(final Object[] state) {
            if (!(Boolean) state[0]) return;
            seen = true;
            flush();
            final Number other = (Number) state[1];
            acc = acc == null ? other : NumberHelper.add(acc, other);
        }

        @Override
        public long estimatedBytes() {
            return 64;
        }
    }

    private static final class ExtremeReducer implements Partial {

        private final CsrExecutionContext ctx;
        private final boolean min;
        private boolean seen;
        private Object acc;
        // the running extreme while every entry so far was a column reference of one integral type
        private boolean fast;
        private ValueType fastType;
        private long fastBest;

        ExtremeReducer(final CsrExecutionContext ctx, final boolean min) {
            this.ctx = ctx;
            this.min = min;
        }

        @Override
        public void add(final Batch b, final int i) {
            final ColumnReader column = integralColumn(ctx, b, i);
            if (column != null && (!seen || (fast && fastType == column.singleType()))) {
                final long v = column.rawBitsAt(b.aux[i]);
                if (!seen) {
                    fastBest = v;
                    fastType = column.singleType();
                    fast = true;
                    seen = true;
                } else if (min ? v < fastBest : v > fastBest) {
                    fastBest = v;
                }
                return;
            }
            if (fast) {
                acc = box(fastType, fastBest);
                fast = false;
            }
            final Object v = Materializer.value(ctx, b, i);
            if (!seen) {
                acc = v;
                seen = true;
            } else {
                acc = min ? Operator.min.apply(acc, v) : Operator.max.apply(acc, v);
            }
        }

        @Override
        public boolean hasResult() {
            return seen;
        }

        @Override
        public Object result() {
            return fast ? box(fastType, fastBest) : acc;
        }

        @Override
        public Object[] state() {
            return new Object[]{seen, result()};
        }

        @Override
        public void combine(final Object[] state) {
            if (!(Boolean) state[0]) return;
            final Object v = state[1];
            if (fast) {
                acc = box(fastType, fastBest);
                fast = false;
            }
            if (!seen) {
                acc = v;
                seen = true;
            } else {
                acc = min ? Operator.min.apply(acc, v) : Operator.max.apply(acc, v);
            }
        }

        @Override
        public long estimatedBytes() {
            return 128;
        }
    }

    /**
     * The arithmetic of {@link MeanGlobalStep.MeanNumber} on a sum and a count that can be read, so that the state can be
     * written out: the sum of the value times its bulk, promoted by {@link NumberHelper}, and the count of the bulk.
     */
    private static final class MeanReducer implements Partial {

        private final CsrExecutionContext ctx;
        private boolean seen;
        private Number sum;
        private long count;

        MeanReducer(final CsrExecutionContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void add(final Batch b, final int i) {
            seen = true;
            final Number value = (Number) Materializer.value(ctx, b, i);
            if (null == value) return;
            final long bulk = b.bulk[i];
            final Number projected = NumberHelper.mul(value, bulk);
            if (sum == null) {
                sum = projected;
                count = bulk;
            } else {
                count += bulk;
                sum = NumberHelper.add(sum, projected);
            }
        }

        @Override
        public boolean hasResult() {
            return seen;
        }

        @Override
        public Object result() {
            return sum == null ? null : NumberHelper.div(sum, count, true);
        }

        @Override
        public Object[] state() {
            return new Object[]{seen, sum, count};
        }

        @Override
        public void combine(final Object[] state) {
            if (!(Boolean) state[0]) return;
            seen = true;
            final Number otherSum = (Number) state[1];
            if (otherSum == null) return;
            final long otherCount = (Long) state[2];
            if (sum == null) {
                sum = otherSum;
                count = otherCount;
            } else {
                count += otherCount;
                sum = NumberHelper.add(sum, otherSum);
            }
        }

        @Override
        public long estimatedBytes() {
            return 96;
        }
    }
}
