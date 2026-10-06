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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.repeat;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;

/**
 * {@code loops()} and {@code loops(name)}: replaces each entry by the loop level that the enclosing fused
 * {@code repeat()} is currently evaluating, keeping the entry's bulk. Inside the body the level is the number of
 * completed iterations, in an {@code until} or {@code emit} test after the body it is one more, as for a traverser of
 * the standard {@code RepeatStep}.
 */
final class LoopsOperator extends EntryStreamOperator {

    private final String name;
    private LoopStack stack;

    LoopsOperator(final Ops.Loops node, final OperatorSpec spec) {
        super(spec);
        this.name = node.name();
    }

    @Override
    protected void onOpen() {
        stack = LoopStack.of(ctx);
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        out.addValue(Integer.valueOf(stack.loops(name)), in.bulk[i]);
        return true;
    }

    @Override
    protected void onClose() {
        stack = null;
    }
}
