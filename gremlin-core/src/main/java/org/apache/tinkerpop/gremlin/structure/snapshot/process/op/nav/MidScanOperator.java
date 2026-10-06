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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

/**
 * Mid-traversal {@code V()} and {@code E()}: every input entry emits the full scan, so each ordinal comes out once with
 * the sum of the input bulks. The input is consumed completely before the scan starts, which keeps the output at
 * one entry per ordinal instead of one scan per input batch.
 */
final class MidScanOperator extends AbstractCsrOperator {

    private final Lane lane;
    private Batch in;
    private boolean scanning;
    private long total;
    private boolean sawInput;
    private int count;
    private int next;

    MidScanOperator(final Sources.MidScan node, final OperatorSpec spec) {
        super(spec);
        this.lane = node.lane();
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        count = lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
        restart();
    }

    private void restart() {
        scanning = false;
        total = 0;
        sawInput = false;
        next = 0;
    }

    @Override
    protected boolean produce(final Batch out) {
        if (!scanning) {
            while (pull(in)) {
                for (int i = 0; i < in.n; i++) total += in.bulk[i];
                sawInput = true;
                ctx.checkInterrupt();
            }
            scanning = true;
            if (!sawInput) return false;
        }
        final int end = (int) Math.min(count, (long) next + out.remaining());
        for (int o = next; o < end; o++) {
            out.ord[out.n] = o;
            out.bulk[out.n++] = total;
        }
        next = end;
        return next < count;
    }

    @Override
    protected void doReset() {
        restart();
    }

    @Override
    protected void doClose() {
        in = null;
    }
}
