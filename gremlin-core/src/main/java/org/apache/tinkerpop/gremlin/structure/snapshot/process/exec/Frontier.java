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
 * A set of vertex or edge ordinals with a merged bulk each, used to merge bulks (the {@code Merge} operator, the level
 * frontiers of {@code repeat()}) and as the state behind dense dedup-like bookkeeping. Adding an ordinal that is
 * already present adds the bulks. Two representations exist, see {@link DenseFrontier} and {@link SparseFrontier}; both
 * reserve their memory from the {@link MemoryBudget} and give it back on {@link #release()}.
 * <p/>
 * Usage is add phase, then {@link #seal()}, then any number of drains: {@link #drain(Batch)} appends entries in
 * ascending ordinal order and resumes where it stopped, {@link #rewind()} starts the drain over, and {@link #clear()}
 * empties the frontier for reuse. Adding after {@link #seal()} is allowed and un-seals it. For a lane {@code E} the
 * frontier does not keep the source vertex, so a region that records sources must not merge edges through a frontier.
 */
public interface Frontier {

    /**
     * The smallest expected-entries to universe ratio that selects the dense representation: dense when
     * {@code expected * DENSE_RATIO >= universe}, which is the {@code V/64} rule of the spike document.
     */
    int DENSE_RATIO = 64;

    Lane lane();

    boolean isDense();

    /**
     * Adds bulk to the ordinal.
     */
    void add(int ordinal, long bulk);

    /**
     * Adds every entry of a batch of this frontier's lane.
     */
    default void addAll(final Batch batch) {
        if (batch.lane != lane()) throw new IllegalArgumentException("Batch lane " + batch.lane + " is not " + lane());
        for (int i = 0; i < batch.n; i++) add(batch.ord[i], batch.bulk[i]);
    }

    /**
     * Makes the frontier ready for draining. For the sparse representation this sorts and merges the entries.
     */
    void seal();

    /**
     * The number of distinct ordinals. Exact only after {@link #seal()} for the sparse representation.
     */
    long distinct();

    default boolean isEmpty() {
        return distinct() == 0;
    }

    /**
     * Appends entries in ascending ordinal order to the batch, which must be of this frontier's lane and have room,
     * until it is full or the frontier is exhausted. Seals the frontier if necessary.
     *
     * @return the number of entries appended, 0 once exhausted
     */
    int drain(Batch out);

    /**
     * Restarts {@link #drain(Batch)} from the smallest ordinal.
     */
    void rewind();

    /**
     * Removes every entry and rewinds, keeping the storage.
     */
    void clear();

    /**
     * The bytes this frontier has reserved from the budget.
     */
    long reservedBytes();

    /**
     * Gives the reserved memory back to the budget. The frontier must not be used afterwards.
     */
    void release();

    /**
     * Creates the representation that suits the expected number of entries: dense when
     * {@code expectedEntries * DENSE_RATIO >= universe} and the budget can cover it, sparse otherwise.
     *
     * @param lane            {@code V} or {@code E}
     * @param universe        the number of vertices or edges
     * @param expectedEntries an estimate of the number of distinct ordinals
     * @throws CsrMemoryBudgetException if not even the sparse representation can reserve its initial memory
     */
    static Frontier create(final MemoryBudget budget, final String owner, final Lane lane, final int universe,
                           final long expectedEntries) {
        if (!lane.isElement()) throw new IllegalArgumentException("A frontier holds vertices or edges, not " + lane);
        if (expectedEntries * DENSE_RATIO >= universe && DenseFrontier.bytesFor(universe) <= budget.available()) {
            return new DenseFrontier(budget, owner, lane, universe);
        }
        return new SparseFrontier(budget, owner, lane, universe);
    }
}
