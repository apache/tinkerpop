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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

/**
 * A {@link BatchSupplier} that hands out a batch the parent operator placed there, once, for running a child
 * {@link CsrPipeline} over the parent's current input. The parent calls {@link #set(Batch)}, then {@code reset()} on
 * the child pipeline, then drains it.
 */
public final class FeedSupplier implements BatchSupplier {

    private Batch pending;

    /**
     * Makes the batch the next, and only, result of {@link #next(Batch)}. The entries are copied at that time, so the
     * caller may keep using the batch.
     */
    public void set(final Batch batch) {
        this.pending = batch;
    }

    @Override
    public boolean next(final Batch out) {
        out.clear();
        if (pending == null || pending.n == 0) return false;
        if (pending.n > out.capacity || pending.lane != out.lane) {
            throw new IllegalStateException("A fed batch of lane " + pending.lane + " and " + pending.n
                    + " entries does not fit a batch of lane " + out.lane + " and capacity " + out.capacity);
        }
        for (int i = 0; i < pending.n; i++) out.copyEntry(pending, i);
        pending = null;
        return true;
    }
}
