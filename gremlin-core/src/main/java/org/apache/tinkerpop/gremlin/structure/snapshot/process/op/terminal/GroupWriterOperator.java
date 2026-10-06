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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * {@code GroupCountSideEffect} and {@code GroupSideEffect}: passes the input through while accumulating like the
 * global form, and writes the map to the side effect once, when the input is exhausted. The write goes through
 * {@code sideEffects.add(key, map)}, so the reducer registered for the key merges it with any initial value. A writer
 * that saw no productive entry writes nothing, like the standard steps.
 */
final class GroupWriterOperator extends AbstractCsrOperator {

    private final String sideEffectKey;
    private GroupAccumulator accumulator;
    private Batch in;
    private int position;
    private boolean finished;

    GroupWriterOperator(final OperatorSpec spec) {
        super(spec);
        this.sideEffectKey = spec.node() instanceof Ops.GroupCountSideEffect
                ? ((Ops.GroupCountSideEffect) spec.node()).sideEffectKey()
                : ((Ops.GroupSideEffect) spec.node()).sideEffectKey();
    }

    @Override
    protected void doOpen() {
        if (ctx.sideEffects() == null) {
            throw new IllegalStateException(describe() + " needs the side effects of a traversal");
        }
        in = spec.newInputBatch(ctx.batchSize());
        if (spec.node() instanceof Ops.GroupCountSideEffect) {
            accumulator = GroupAccumulator.counts(ctx, ((Ops.GroupCountSideEffect) spec.node()).key(),
                    spec.inputLane(), owner("state"), true);
        } else {
            final Ops.GroupSideEffect group = (Ops.GroupSideEffect) spec.node();
            accumulator = GroupAccumulator.reduced(ctx, group.key(), group.reducer(), spec.inputLane(), owner("state"));
        }
    }

    @Override
    protected boolean produce(final Batch out) {
        while (true) {
            while (position < in.n && !out.isFull()) out.copyEntry(in, position++);
            if (position < in.n) return true;
            if (finished) return false;
            if (!pull(in)) {
                finished = true;
                in.n = 0;
                if (!accumulator.isEmpty()) ctx.sideEffects().add(sideEffectKey, accumulator.build());
                return false;
            }
            accumulator.add(in);
            position = 0;
            if (out.isFull()) return true;
        }
    }

    @Override
    protected void doReset() {
        accumulator.reset();
        in.n = 0;
        position = 0;
        finished = false;
    }

    @Override
    protected void doClose() {
        if (accumulator != null) accumulator.close();
        accumulator = null;
        in = null;
    }
}
