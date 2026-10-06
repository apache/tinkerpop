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

/**
 * {@code Count}, {@code Sum}, {@code Min}, {@code Max}, {@code Mean} and {@code Fold}: consumes the whole input into a
 * {@link Reducers.Reducer} and emits its single result as a SCALAR entry. Count and fold emit for empty input (0 and an
 * empty list); sum, min, max and mean emit nothing.
 */
final class ReducerOperator extends AbstractCsrOperator {

    private Reducers.Reducer reducer;
    private Batch in;
    private boolean done;

    ReducerOperator(final OperatorSpec spec) {
        super(spec);
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        reducer = Reducers.create(spec.node(), ctx, owner("state"));
    }

    @Override
    protected boolean produce(final Batch out) {
        if (done) return false;
        while (pull(in)) {
            reducer.addAll(in);
            ctx.checkInterrupt();
        }
        done = true;
        if (reducer.hasResult()) out.addValue(reducer.result(), 1L);
        return false;
    }

    @Override
    protected void doReset() {
        ctx.budget().releaseAll(owner("state"));
        reducer = Reducers.create(spec.node(), ctx, owner("state"));
        done = false;
    }

    @Override
    protected void doClose() {
        ctx.budget().releaseAll(owner("state"));
        reducer = null;
        in = null;
    }
}
