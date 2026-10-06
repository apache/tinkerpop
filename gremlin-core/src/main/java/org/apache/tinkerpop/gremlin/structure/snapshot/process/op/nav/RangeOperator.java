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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.Arrays;

/**
 * {@code limit}, {@code range}, {@code skip}: passes the entries whose rank, counting bulk, is in {@code [lo, hi)}, and
 * splits the bulk of an entry that straddles a boundary exactly as {@code RangeGlobalStep} does. Once {@code hi} is
 * reached the operator stops pulling, so the source upstream stops too.
 * <p/>
 * With {@code perLevel} the operator keeps one counter for each loop level, like the per-loop counters of the step: the
 * operator that drives a loop calls {@link #setLevel(int)} before it feeds the next level, and an exhausted level
 * drops its remaining input without ending the stream. {@code reset()} clears all counters and sets level 0.
 */
public final class RangeOperator extends EntryStreamOperator {

    private final long lo;
    private final long hi;
    private final boolean perLevel;
    private long single;
    private long[] levels = new long[4];
    private int level;

    RangeOperator(final Ops.Range node, final OperatorSpec spec) {
        super(spec);
        this.lo = node.lo();
        this.hi = node.hi();
        this.perLevel = node.perLevel();
    }

    /**
     * Selects the counter of the given loop level, for a {@code perLevel} range.
     */
    public void setLevel(final int newLevel) {
        if (newLevel < 0) throw new IllegalArgumentException("A loop level is not negative");
        if (newLevel >= levels.length) levels = Arrays.copyOf(levels, Math.max(newLevel + 1, levels.length * 2));
        level = newLevel;
    }

    private long counter() {
        return perLevel ? levels[level] : single;
    }

    private void counter(final long value) {
        if (perLevel) levels[level] = value;
        else single = value;
    }

    @Override
    protected boolean produce(final Batch out) {
        if (!perLevel && hi >= 0 && single >= hi) return false;
        return super.produce(out);
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        long count = counter();
        if (hi >= 0 && count >= hi) return perLevel;
        final long avail = in.bulk[i];
        if (count + avail <= lo) {
            counter(count + avail);
            return true;
        }
        final long toSkip = count < lo ? lo - count : 0;
        final long toTrim = hi >= 0 && count + avail >= hi ? count + avail - hi : 0;
        final long toEmit = avail - toSkip - toTrim;
        count += toSkip + toEmit;
        counter(count);
        out.copyEntry(in, i, toEmit);
        return perLevel || hi < 0 || count < hi;
    }

    @Override
    protected void onReset() {
        single = 0;
        Arrays.fill(levels, 0L);
        level = 0;
    }
}
