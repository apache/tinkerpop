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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

/**
 * {@code group().by(key).by(reducer)} with state that is bounded by the memory budget, see {@link SpillGroupState}: a
 * partial reducer state per key for count, sum, min, max and mean, and an external sort of the members, streamed into the
 * reducer pipeline, for the other reducers. Emits the result map as one SCALAR entry.
 */
final class SpillGroupOperator extends AbstractCsrOperator {

    private final Terminals.Group node;

    private SpillGroupState state;
    private Batch in;
    private boolean done;

    SpillGroupOperator(final Terminals.Group node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        state = new SpillGroupState(ctx, spec, stats(), describe(), node.key(), node.reducer());
    }

    @Override
    protected boolean produce(final Batch out) {
        if (done) return false;
        while (pull(in)) state.add(in);
        done = true;
        out.addValue(state.build(), 1);
        return true;
    }

    @Override
    protected void doReset() {
        state.reset();
        in.clear();
        done = false;
    }

    @Override
    protected void doClose() {
        if (state != null) {
            final SpillGroupState s = state;
            state = null;
            s.close();
        }
    }
}
