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

import java.util.List;

/**
 * {@code And} and {@code Or}: evaluates the children in order and stops at the first that decides the entry. Each
 * child is fed bulk 1 and the entry passes with its own bulk.
 */
final class ConnectiveOperator extends EntryStreamOperator {

    private final List<CsrPlan> plans;
    private final boolean and;
    private ExistenceTest[] tests;

    ConnectiveOperator(final List<CsrPlan> plans, final boolean and, final OperatorSpec spec) {
        super(spec);
        this.plans = plans;
        this.and = and;
    }

    @Override
    protected void onOpen() {
        tests = new ExistenceTest[plans.size()];
        try {
            for (int k = 0; k < tests.length; k++) tests[k] = ExistenceTest.create(ctx, plans.get(k), owner("child " + k));
        } catch (RuntimeException e) {
            closeTests();
            throw e;
        }
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        boolean pass = and;
        for (final ExistenceTest test : tests) {
            final boolean result = test.test(in, i);
            if (and ? !result : result) {
                pass = !and;
                break;
            }
        }
        if (pass) out.copyEntry(in, i);
        return true;
    }

    @Override
    protected void onReset() {
        for (final ExistenceTest test : tests) test.reset();
    }

    @Override
    protected void onClose() {
        closeTests();
    }

    private void closeTests() {
        RuntimeException failure = null;
        if (tests != null) {
            for (final ExistenceTest test : tests) {
                if (test == null) continue;
                try {
                    test.close();
                } catch (RuntimeException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
            }
        }
        tests = null;
        if (failure != null) throw failure;
    }
}
