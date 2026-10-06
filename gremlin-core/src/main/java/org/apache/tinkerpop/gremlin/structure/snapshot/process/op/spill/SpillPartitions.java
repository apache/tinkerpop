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
 * Hash-partitions records into {@link SpillRun}s on scratch, one run per partition, each written in arrival order. The
 * records start with a type byte, then the length and bytes of the canonical key, which is what is hashed. The write
 * buffers of all partitions are reserved for the lifetime of the object.
 */
final class SpillPartitions {

    static final int FANOUT = 16;

    private final SpillRun.Writer[] writers = new SpillRun.Writer[FANOUT];
    private final CsrExecutionContext ctx;
    private final int seed;
    private SpillRun[] finished;

    SpillPartitions(final CsrExecutionContext ctx, final String owner, final String name, final int seed) {
        this.ctx = ctx;
        this.seed = seed;
        final int buffer = SpillSupport.bufferBytes(ctx, 4 * FANOUT);
        try {
            for (int p = 0; p < FANOUT; p++) {
                writers[p] = new SpillRun.Writer(ctx, owner, name + "-p" + p, buffer);
            }
        } catch (RuntimeException e) {
            abort();
            throw e;
        }
    }

    static int partitionOf(final byte[] record, final int seed) {
        final int keyLength = new ByteSource(record, 1).getInt();
        return (KeyBytes.partitionHash(record, 5, keyLength, seed) >>> 1) % FANOUT;
    }

    void append(final byte[] record) {
        writers[partitionOf(record, seed)].writeRecord(record);
    }

    /**
     * Flushes the writers; the entry of an empty partition is null.
     */
    SpillRun[] finish() {
        if (finished == null) {
            finished = new SpillRun[FANOUT];
            for (int p = 0; p < FANOUT; p++) {
                final SpillRun run = writers[p].finish();
                finished[p] = run.records() == 0 ? null : run;
                if (finished[p] == null) run.discard(ctx.scratch());
            }
        }
        return finished;
    }

    void abort() {
        for (final SpillRun.Writer w : writers) {
            if (w != null) w.abort();
        }
        finished = null;
    }

    /**
     * Splits a run that does not fit into partitions by a different hash; the input run is deleted.
     */
    static SpillRun[] split(final CsrExecutionContext ctx, final String owner, final String name, final SpillRun run,
                            final int seed) {
        final SpillPartitions parts = new SpillPartitions(ctx, owner, name, seed);
        try {
            final SpillRun.Reader reader = new SpillRun.Reader(ctx, run, owner, SpillSupport.bufferBytes(ctx, 4));
            try {
                byte[] record;
                while ((record = reader.next()) != null) {
                    parts.append(record);
                    ctx.checkInterrupt();
                }
            } finally {
                reader.close();
            }
            final SpillRun[] result = parts.finish();
            run.discard(ctx.scratch());
            return result;
        } catch (RuntimeException e) {
            parts.abort();
            throw e;
        }
    }
}
