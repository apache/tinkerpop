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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;

/**
 * Budget headroom that a spillable operator keeps for the moment it has to spill. Spilling needs memory (write buffers)
 * at the very moment the operator's own state has used up what the budget could give, so an operator holds a small
 * reserve from {@code doOpen} on, under its own owner, and hands it over just before it builds its
 * {@link SpillPartitions} or {@link SpillRun.Writer}: {@link #handOver()} releases the bytes, the writers then reserve
 * theirs from the same budget, and nothing can take the bytes in between because execution is single-threaded.
 * <p>
 * A reserve is best effort. If the budget cannot cover it at open, {@link #hold()} returns false and the operator
 * behaves as it did without one (it may fail to spill if the budget is then exactly full).
 * <pre>
 * reserve = new SpillReserve(ctx, owner("spill reserve"), SpillReserve.PARTITIONS);   // in doOpen
 * reserve.hold();
 * ...
 * reserve.handOver();                       // state could not grow: free the headroom
 * partitions = new SpillPartitions(...);    // ... and use it
 * ...
 * reserve.retake();                         // optional, after the writers released their buffers
 * reserve.release();                        // in clearState (and hold() again in doReset)
 * </pre>
 */
public final class SpillReserve {

    /**
     * Headroom for hash-partitioning: {@code SpillPartitions.FANOUT} write buffers of the minimum size.
     */
    public static final long PARTITIONS = 16L * SpillSupport.MIN_BUFFER;
    /**
     * Headroom for one sorted run writer of the minimum buffer size.
     */
    public static final long RUN = SpillSupport.MIN_BUFFER;

    private final CsrExecutionContext ctx;
    private final String owner;
    private final long bytes;
    private boolean held;

    public SpillReserve(final CsrExecutionContext ctx, final String owner, final long bytes) {
        this.ctx = ctx;
        this.owner = owner;
        this.bytes = bytes;
    }

    /**
     * Reserves the headroom if the budget can cover it.
     *
     * @return whether the reserve is held now
     */
    public boolean hold() {
        if (!held) held = ctx.budget().tryReserve(bytes, owner);
        return held;
    }

    /**
     * Releases the headroom so that the caller can reserve write buffers from it; does nothing if it is not held.
     */
    public void handOver() {
        release();
    }

    /**
     * Takes the headroom again after the write buffers were released; best effort like {@link #hold()}.
     */
    public boolean retake() {
        return hold();
    }

    /**
     * Releases the headroom for good (or until the next {@link #hold()}).
     */
    public void release() {
        if (held) {
            ctx.budget().release(bytes, owner);
            held = false;
        }
    }
}
