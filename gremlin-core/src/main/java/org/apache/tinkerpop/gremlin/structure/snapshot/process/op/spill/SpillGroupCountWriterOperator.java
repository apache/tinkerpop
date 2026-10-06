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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * {@code groupCount('x')} with state that is bounded by the memory budget. The entries pass through; the counts are kept
 * in a {@link SpillGroupCountState} and added to the side effect once, when the input is exhausted. A non-productive key
 * throws, like the standard step.
 */
final class SpillGroupCountWriterOperator extends AbstractCsrOperator {

    private final Ops.GroupCountSideEffect node;

    private SpillGroupCountState state;
    private Batch in;
    private int position;
    private boolean finished;

    SpillGroupCountWriterOperator(final Ops.GroupCountSideEffect node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
    }

    @Override
    protected void doOpen() {
        if (ctx.sideEffects() == null) {
            throw new IllegalStateException(describe() + " needs the side effects of a traversal");
        }
        in = spec.newInputBatch(ctx.batchSize());
        state = new SpillGroupCountState(ctx, stats(), describe(), node.key(), spec.inputLane(), true);
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
                position = 0;
                if (!state.isEmpty()) ctx.sideEffects().add(node.sideEffectKey(), state.build());
                return false;
            }
            state.add(in);
            position = 0;
            if (out.isFull()) return true;
        }
    }

    @Override
    protected void doReset() {
        state.reset();
        in.n = 0;
        position = 0;
        finished = false;
    }

    @Override
    protected void doClose() {
        if (state != null) {
            final SpillGroupCountState s = state;
            state = null;
            s.close();
        }
    }
}
