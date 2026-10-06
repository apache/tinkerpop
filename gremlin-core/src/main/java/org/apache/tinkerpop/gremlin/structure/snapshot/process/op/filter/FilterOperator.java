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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.filter;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Preds;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;

/**
 * Keeps the entries that satisfy a predicate, with their bulk and source unchanged. The predicate is bound to the
 * execution when the operator opens and again after {@code reset()}, which also covers the lane test of
 * {@code ClassFilterStep} ({@link Preds.LaneType}).
 */
final class FilterOperator extends EntryStreamOperator {

    private final Preds.Pred pred;
    private EntryPredicate predicate;

    FilterOperator(final Ops.Filter node, final OperatorSpec spec) {
        super(spec);
        this.pred = node.pred();
    }

    @Override
    protected void onOpen() {
        bind();
    }

    private void bind() {
        predicate = PredicateCompiler.compile(pred, ctx, spec.inputLane());
    }

    @Override
    protected void onReset() {
        bind();
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        if (predicate.test(in, i)) out.copyEntry(in, i);
        return true;
    }

    @Override
    protected void onClose() {
        predicate = null;
    }
}
