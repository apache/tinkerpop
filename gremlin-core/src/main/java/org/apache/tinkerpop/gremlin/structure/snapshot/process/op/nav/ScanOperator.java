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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

/**
 * {@code g.V()} and {@code g.E()}: every ordinal in ascending order with bulk 1, emitted in bounded batches.
 */
final class ScanOperator extends AbstractCsrOperator {

    private final Lane lane;
    private int count;
    private int next;

    ScanOperator(final Sources.Scan node, final OperatorSpec spec) {
        super(spec);
        this.lane = node.lane();
    }

    @Override
    protected void doOpen() {
        count = lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
        next = 0;
    }

    @Override
    protected boolean produce(final Batch out) {
        final int end = (int) Math.min(count, (long) next + out.remaining());
        final int[] ord = out.ord;
        final long[] bulk = out.bulk;
        int n = out.n;
        for (int o = next; o < end; o++) {
            ord[n] = o;
            bulk[n++] = 1L;
        }
        out.n = n;
        next = end;
        return next < count;
    }

    @Override
    protected void doReset() {
        next = 0;
    }

    @Override
    protected void doClose() {
    }
}
