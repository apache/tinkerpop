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

/**
 * {@code Sort}: keeps every productive entry with its sort keys in the heap, under a budget reservation, sorts stably
 * and emits the entries in order with their own bulks. The spill-to-scratch version replaces this operator.
 */
final class SortOperator extends AbstractCsrOperator {

    private OrderedRows rows;
    private Batch in;
    private List<OrderedRows.Row> sorted;
    private int position;

    SortOperator(final OperatorSpec spec) {
        super(spec);
    }

    @Override
    protected void doOpen() {
        final Terminals.Sort sort = (Terminals.Sort) spec.node();
        in = spec.newInputBatch(ctx.batchSize());
        rows = new OrderedRows(ctx, sort.keys(), sort.orders(), spec.inputLane());
    }

    @Override
    protected boolean produce(final Batch out) {
        if (sorted == null) {
            sorted = new ArrayList<>();
            final long bytes = rows.rowBytes();
            while (pull(in)) {
                for (int i = 0; i < in.n; i++) {
                    final OrderedRows.Row row = rows.row(in, i);
                    if (row == null) continue;
                    ctx.budget().reserve(bytes, owner("rows"));
                    sorted.add(row);
                }
                ctx.checkInterrupt();
            }
            sorted.sort(rows.order());
        }
        while (position < sorted.size() && !out.isFull()) {
            final OrderedRows.Row row = sorted.get(position);
            sorted.set(position++, null);
            OrderedRows.emit(out, row, row.bulk);
        }
        if (position >= sorted.size()) {
            ctx.budget().releaseAll(owner("rows"));
            return false;
        }
        return true;
    }

    @Override
    protected void doReset() {
        ctx.budget().releaseAll(owner("rows"));
        sorted = null;
        position = 0;
        rows.reset();
    }

    @Override
    protected void doClose() {
        ctx.budget().releaseAll(owner("rows"));
        sorted = null;
        if (rows != null) rows.close();
        rows = null;
        in = null;
    }
}
