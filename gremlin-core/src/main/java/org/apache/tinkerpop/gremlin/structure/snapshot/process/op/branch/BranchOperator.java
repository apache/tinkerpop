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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;

/**
 * The base of the operators that run child pipelines and forward their results. A subclass {@link #advance()}s to the
 * next run of results, which it exposes as a window {@code [curPos, curEnd)} of a batch; this class copies the window to
 * the output, multiplying the bulk, and resumes where it stopped when the output batch is full.
 */
abstract class BranchOperator extends AbstractCsrOperator {

    /**
     * The batch the window reads from, in the output lane.
     */
    protected Batch cur;
    protected int curPos;
    protected int curEnd;
    /**
     * Multiplies the bulk of the window's entries, when {@link #curFixedBulk} is false.
     */
    protected long curMult = 1L;
    protected boolean curFixedBulk;
    /**
     * The bulk of every entry of the window when {@link #curFixedBulk} is true.
     */
    protected long curBulk = 1L;

    protected BranchOperator(final OperatorSpec spec) {
        super(spec);
    }

    /**
     * Moves to the next window of results, or to the end of the stream.
     *
     * @return false when there are no more results; true if a window was set (which may be empty)
     */
    protected abstract boolean advance();

    @Override
    protected final boolean produce(final Batch out) {
        while (true) {
            while (curPos < curEnd) {
                if (out.isFull()) return true;
                out.copyEntry(cur, curPos, curFixedBulk ? curBulk : cur.bulk[curPos] * curMult);
                curPos++;
            }
            if (out.isFull()) return true;
            if (!advance()) return false;
            ctx.checkInterrupt();
        }
    }

    protected final void setWindow(final Batch batch, final int from, final int to, final long mult) {
        cur = batch;
        curPos = from;
        curEnd = to;
        curMult = mult;
        curFixedBulk = false;
    }

    protected final void setFixedWindow(final Batch batch, final int from, final int to, final long bulk) {
        cur = batch;
        curPos = from;
        curEnd = to;
        curBulk = bulk;
        curFixedBulk = true;
    }

    protected final void clearWindow() {
        cur = null;
        curPos = 0;
        curEnd = 0;
    }
}
