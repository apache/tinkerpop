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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * {@code TopK}: {@code order().by(...).range(lo, hi)} with a bounded heap. The heap keeps the best entries whose
 * bulks add up to {@code hi}, so it holds at most {@code hi} entries; the worst retained entry is trimmed to the
 * bulk that still counts. Ties are broken by arrival, like the stable sort of the standard step, and entries whose
 * sort keys are non-productive are dropped. The emitted entries are ranks {@code [lo, hi)} counting bulk, each with
 * the bulk that falls inside the window. The heap grows entry by entry under a budget reservation, so an
 * unreasonable {@code hi} fails with a {@code CsrMemoryBudgetException} rather than allocating up front.
 */
final class TopKOperator extends AbstractCsrOperator {

    private OrderedRows rows;
    private Batch in;
    private long lo;
    private long hi;
    private PriorityQueue<OrderedRows.Row> heap;
    private long heapBulk;
    private List<OrderedRows.Row> window;
    private int position;

    TopKOperator(final OperatorSpec spec) {
        super(spec);
    }

    @Override
    protected void doOpen() {
        final Terminals.TopK topK = (Terminals.TopK) spec.node();
        in = spec.newInputBatch(ctx.batchSize());
        rows = new OrderedRows(ctx, topK.keys(), topK.orders(), spec.inputLane());
        lo = topK.lo();
        hi = topK.hi();
        newHeap();
    }

    private void newHeap() {
        // the worst entry first
        heap = new PriorityQueue<>(16, rows.order().reversed());
        heapBulk = 0;
    }

    @Override
    protected boolean produce(final Batch out) {
        if (window == null) {
            consume();
            buildWindow();
        }
        while (position < window.size() && !out.isFull()) {
            final OrderedRows.Row row = window.get(position);
            window.set(position++, null);
            OrderedRows.emit(out, row, row.bulk);
        }
        if (position >= window.size()) {
            ctx.budget().releaseAll(owner("heap"));
            return false;
        }
        return true;
    }

    private void consume() {
        final long bytes = rows.rowBytes();
        while (pull(in)) {
            for (int i = 0; i < in.n; i++) {
                if (hi == 0) continue;
                final OrderedRows.Row row = rows.row(in, i);
                if (row == null) continue;
                // the heap is full and the entry is not better than its worst one: its ranks are beyond hi
                if (heapBulk >= hi && rows.order().compare(row, heap.peek()) > 0) continue;
                ctx.budget().reserve(bytes, owner("heap"));
                heap.add(row);
                heapBulk += row.bulk;
                while (heapBulk > hi) {
                    final OrderedRows.Row worst = heap.peek();
                    final long excess = heapBulk - hi;
                    if (worst.bulk <= excess) {
                        heap.poll();
                        heapBulk -= worst.bulk;
                        ctx.budget().release(bytes, owner("heap"));
                    } else {
                        worst.bulk -= excess;
                        heapBulk = hi;
                    }
                }
            }
            ctx.checkInterrupt();
        }
    }

    private void buildWindow() {
        final List<OrderedRows.Row> all = new ArrayList<>(heap);
        heap.clear();
        all.sort(rows.order());
        window = new ArrayList<>();
        long rank = 0;
        for (final OrderedRows.Row row : all) {
            if (rank >= hi) break;
            final long bulk = row.bulk;
            final long from = Math.max(rank, lo);
            final long to = Math.min(rank + bulk, hi);
            if (to > from) {
                row.bulk = to - from;
                window.add(row);
            }
            rank += bulk;
        }
        ctx.budget().releaseAll(owner("heap"));
        ctx.budget().reserve(rows.rowBytes() * window.size(), owner("heap"));
    }

    @Override
    protected void doReset() {
        ctx.budget().releaseAll(owner("heap"));
        newHeap();
        window = null;
        position = 0;
        rows.reset();
    }

    @Override
    protected void doClose() {
        ctx.budget().releaseAll(owner("heap"));
        heap = null;
        window = null;
        if (rows != null) rows.close();
        rows = null;
        in = null;
    }
}
