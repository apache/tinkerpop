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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;

/**
 * The runtime form of one IR node. Operators form a chain in which each one <em>pulls</em> batches from its upstream
 * operator, and the {@code CsrSuperStep} pulls from the last one. A pull keeps execution lazy, lets a flat-mapping
 * operator suspend on a cursor when its output batch is full so a hub vertex never builds an unbounded batch, and makes
 * early termination natural: an operator that is done stops pulling. (The spike document says "pull at the boundary,
 * push inside"; a pull chain gives the same bounded, resumable behavior with one simple contract.)
 * <p/>
 * Lifecycle, driven by {@link CsrPipeline}: {@link #open} once, then any number of {@link #next} calls until one
 * returns false, then {@link #reset} to run again over new input, and finally {@link #close}. Operators are created by
 * the {@link CsrOperatorFactory} from an {@link OperatorSpec}, hold no graph reference until {@code open}, and are not
 * thread-safe. Implement by extending {@link AbstractCsrOperator} rather than this interface directly.
 */
public interface CsrOperator extends AutoCloseable {

    /**
     * The IR node this operator executes.
     */
    CsrOp node();

    /**
     * The lane of the batches {@link #next} fills.
     */
    Lane outputLane();

    /**
     * Whether the emitted {@code E} batches carry the source vertex.
     */
    boolean outputRecordsSource();

    /**
     * A new empty batch of the shape {@link #next} expects.
     */
    Batch newOutputBatch(int capacity);

    /**
     * Allocates per-execution state: buffers, bitsets (after reserving them from the budget), child pipelines, bound
     * {@code P} constants. The upstream operator is opened first by the pipeline.
     */
    void open(CsrExecutionContext ctx);

    /**
     * Clears {@code out} and fills it with the next entries, at least one.
     *
     * @param out a batch of {@link #newOutputBatch}'s shape
     * @return false, with {@code out} empty, when the operator is exhausted
     */
    boolean next(Batch out);

    /**
     * Returns to the state right after {@link #open}: forgets emitted and accumulated state (dedup sets, counters,
     * frontiers) and re-reads bound constants, keeping allocations. The pipeline resets upstream first.
     */
    void reset();

    /**
     * Releases everything {@link #open} reserved or allocated, including budget reservations and scratch files.
     * Idempotent.
     */
    @Override
    void close();

    /**
     * The counters of this operator.
     */
    OperatorStats stats();
}
