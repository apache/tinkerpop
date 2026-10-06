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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;

/**
 * Feeds the {@code Input} of a child pipeline either from one batch of the parent or from a replay of retained entries
 * (a {@link org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillBuffer} that may be backed by a run).
 * The entries are copied when the child pulls them, so the parent may reuse its batch at once.
 */
final class ChunkSupplier implements BatchSupplier {

    private Batch single;
    private BatchSupplier replay;

    /**
     * Makes the batch the next, and only, result of {@link #next(Batch)}.
     */
    void set(final Batch batch) {
        single = batch;
        replay = null;
    }

    /**
     * Makes the results of the supplier the results of {@link #next(Batch)}, in order. The supplier fills the batch
     * itself, so it may be backed by a run.
     */
    void setReplay(final BatchSupplier source) {
        single = null;
        replay = source;
    }

    @Override
    public boolean next(final Batch out) {
        out.clear();
        if (replay != null) return replay.next(out);
        while (true) {
            final Batch source;
            if (single != null) {
                source = single;
                single = null;
            } else {
                return false;
            }
            if (source.n == 0) continue;
            if (source.n > out.capacity || source.lane != out.lane) {
                throw new IllegalStateException("A fed batch of lane " + source.lane + " and " + source.n
                        + " entries does not fit a batch of lane " + out.lane + " and capacity " + out.capacity);
            }
            for (int i = 0; i < source.n; i++) out.copyEntry(source, i);
            return true;
        }
    }
}
