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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;

/**
 * The base of the operators that emit any number of entries for one input entry, such as {@code values()} and
 * {@code labels()}. It keeps the position in the current input batch; a subclass keeps the position inside the current
 * entry, so that a full output batch resumes exactly where it stopped.
 */
abstract class FanOutOperator extends AbstractCsrOperator {

    private Batch in;
    private int pos;
    private boolean started;

    FanOutOperator(final OperatorSpec spec) {
        super(spec);
    }

    /**
     * Called by {@code doOpen}, after the input batch exists.
     */
    protected void onOpen() {
    }

    protected void onClose() {
    }

    /**
     * Called once before the first {@link #emit} for entry {@code i} of the input.
     */
    protected abstract void begin(Batch in, int i);

    /**
     * Appends the next entries for input entry {@code i}, stopping when {@code out} is full.
     *
     * @return true if the entry is finished, false if the batch filled up and the entry has more
     */
    protected abstract boolean emit(Batch in, int i, Batch out);

    @Override
    protected final void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        pos = 0;
        started = false;
        onOpen();
    }

    @Override
    protected final boolean produce(final Batch out) {
        while (true) {
            if (pos >= in.n) {
                if (!pull(in)) return false;
                pos = 0;
                started = false;
            }
            while (pos < in.n) {
                if (out.isFull()) return true;
                if (!started) {
                    begin(in, pos);
                    started = true;
                }
                if (!emit(in, pos, out)) return true;
                pos++;
                started = false;
            }
            ctx.checkInterrupt();
        }
    }

    @Override
    protected void doReset() {
        in.n = 0;
        pos = 0;
        started = false;
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
