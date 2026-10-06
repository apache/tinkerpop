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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.List;

/**
 * {@code Union}: every entry goes to every branch with its bulk, and the outputs of the branches are concatenated
 * per input batch (allowed by the ordering contract). See {@link RoutingOperator} for stateful branches.
 */
final class UnionOperator extends RoutingOperator {

    private Retention shared;

    UnionOperator(final List<CsrPlan> plans, final OperatorSpec spec) {
        super(plans, spec);
    }

    @Override
    protected void doOpen() {
        openRuns(true);
        shared = null;
        for (final Retention r : retention) {
            if (r != null) {
                shared = r;
                break;
            }
        }
    }

    @Override
    protected Batch batchFor(final int branch) {
        return in;
    }

    @Override
    protected void route(final Batch batch) {
        if (shared == null) return;
        for (int i = 0; i < batch.n; i++) shared.add(batch, i);
    }

    @Override
    protected void closeRuns() {
        shared = null;
        super.closeRuns();
    }
}
