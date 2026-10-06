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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

/**
 * {@code GroupCount} and {@code Group}: consumes the whole input into a {@link GroupAccumulator} and emits the result
 * map as one SCALAR entry. An empty input gives an empty map, like the standard steps.
 */
final class GroupTerminalOperator extends AbstractCsrOperator {

    private GroupAccumulator accumulator;
    private Batch in;
    private boolean done;

    GroupTerminalOperator(final OperatorSpec spec) {
        super(spec);
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        if (spec.node() instanceof Terminals.GroupCount) {
            accumulator = GroupAccumulator.counts(ctx, ((Terminals.GroupCount) spec.node()).key(), spec.inputLane(),
                    owner("state"), false);
        } else {
            final Terminals.Group group = (Terminals.Group) spec.node();
            accumulator = GroupAccumulator.reduced(ctx, group.key(), group.reducer(), spec.inputLane(), owner("state"));
        }
    }

    @Override
    protected boolean produce(final Batch out) {
        if (done) return false;
        while (pull(in)) {
            accumulator.add(in);
            ctx.checkInterrupt();
        }
        done = true;
        out.addValue(accumulator.build(), 1L);
        return false;
    }

    @Override
    protected void doReset() {
        accumulator.reset();
        done = false;
    }

    @Override
    protected void doClose() {
        if (accumulator != null) accumulator.close();
        accumulator = null;
        in = null;
    }
}
