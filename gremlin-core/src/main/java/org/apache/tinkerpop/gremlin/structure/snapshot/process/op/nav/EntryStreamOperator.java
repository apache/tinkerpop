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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;

/**
 * The base of the operators that handle their input one entry at a time and emit at most a fixed number of entries per
 * input entry: filters, range, dedup, endpoints. It pulls input batches, keeps the position in the current one so that
 * a full output batch resumes at the next entry, and lets subclasses decide per entry what to emit.
 */
public abstract class EntryStreamOperator extends AbstractCsrOperator {

    private Batch in;
    private int pos;

    protected EntryStreamOperator(final OperatorSpec spec) {
        super(spec);
    }

    /**
     * Called by {@code doOpen}, after the input batch exists.
     */
    protected void onOpen() {
    }

    protected void onReset() {
    }

    protected void onClose() {
    }

    /**
     * Called once for each pulled input batch, before its entries are processed.
     */
    protected void onBatch(final Batch batch) {
    }

    /**
     * The most entries {@link #process} appends for one input entry.
     */
    protected int maxEmitPerEntry() {
        return 1;
    }

    /**
     * Handles entry {@code i} of the input, appending to {@code out} at most {@link #maxEmitPerEntry()} entries.
     *
     * @return false if the operator needs no further input, which ends the stream after the current output batch
     */
    protected abstract boolean process(Batch in, int i, Batch out);

    @Override
    protected final void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        pos = 0;
        onOpen();
    }

    @Override
    protected boolean produce(final Batch out) {
        final int room = maxEmitPerEntry();
        while (true) {
            if (pos >= in.n) {
                if (!pull(in)) return false;
                pos = 0;
                onBatch(in);
                continue;
            }
            while (pos < in.n) {
                if (out.n > 0 && out.remaining() < room) return true;
                final int i = pos++;
                if (!process(in, i, out)) return false;
            }
            if (out.isFull()) return true;
            ctx.checkInterrupt();
        }
    }

    @Override
    protected final void doReset() {
        in.n = 0;
        pos = 0;
        onReset();
    }

    @Override
    protected final void doClose() {
        try {
            onClose();
        } finally {
            in = null;
        }
    }
}
