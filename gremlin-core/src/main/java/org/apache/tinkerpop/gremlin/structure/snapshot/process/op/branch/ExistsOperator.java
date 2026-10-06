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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;

/**
 * {@code Exists} and {@code NotExists}: {@code filter(t)}, {@code where(t)} and {@code not(t)}. The child is fed bulk
 * 1 and the entry passes with its own bulk.
 */
final class ExistsOperator extends EntryStreamOperator {

    private final CsrPlan plan;
    private final boolean negate;
    private ExistenceTest test;

    ExistsOperator(final CsrPlan plan, final boolean negate, final OperatorSpec spec) {
        super(spec);
        this.plan = plan;
        this.negate = negate;
    }

    @Override
    protected void onOpen() {
        test = ExistenceTest.create(ctx, plan, owner("child"));
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        if (test.test(in, i) != negate) out.copyEntry(in, i);
        return true;
    }

    @Override
    protected void onReset() {
        test.reset();
    }

    @Override
    protected void onClose() {
        if (test != null) test.close();
        test = null;
    }
}
