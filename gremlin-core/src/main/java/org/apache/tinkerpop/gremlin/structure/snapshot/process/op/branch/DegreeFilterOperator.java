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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.EntryStreamOperator;

/**
 * The operator of {@link DegreeFilter}. The outcome is memoized per vertex, since a label scan costs the degree.
 */
final class DegreeFilterOperator extends EntryStreamOperator {

    private final DegreeFilter node;
    private CsrSnapshot snapshot;
    private P<Object> predicate;
    private boolean[] labelFlags;
    private boolean nothing;
    private OrdinalBits memo;

    DegreeFilterOperator(final DegreeFilter node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void onOpen() {
        snapshot = ctx.snapshot();
        predicate = (P<Object>) node.predicate();
        final int[] codes = node.labelCodes();
        nothing = false;
        labelFlags = null;
        if (codes != null) {
            if (codes.length == 0) {
                nothing = true;
            } else {
                labelFlags = new boolean[snapshot.edgeLabels().size()];
                for (final int code : codes) {
                    if (code >= 0 && code < labelFlags.length) labelFlags[code] = true;
                }
            }
        }
        memo = labelFlags == null ? null : OrdinalBits.tryCreate(ctx, Plans.universe(snapshot, Lane.V), owner("memo"));
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        final int vertex = in.ord[i];
        boolean pass;
        final int known = memo == null ? -1 : memo.get(vertex);
        if (known >= 0) {
            pass = known == 1;
        } else {
            pass = predicate.test(degree(vertex));
            if (memo != null) memo.put(vertex, pass);
        }
        if (pass) out.copyEntry(in, i);
        return true;
    }

    private long degree(final int vertex) {
        if (nothing) return 0L;
        final Direction direction = node.direction();
        long degree = 0;
        if (direction != Direction.IN) {
            final long start = snapshot.outStart(vertex);
            final long end = snapshot.outEnd(vertex);
            if (labelFlags == null) degree += end - start;
            else {
                for (long p = start; p < end; p++) {
                    if (matches(snapshot.outEdge(p))) degree++;
                }
            }
        }
        if (direction != Direction.OUT) {
            final long start = snapshot.inStart(vertex);
            final long end = snapshot.inEnd(vertex);
            if (labelFlags == null) degree += end - start;
            else {
                for (long p = start; p < end; p++) {
                    if (matches(snapshot.inEdge(p))) degree++;
                }
            }
        }
        return degree;
    }

    private boolean matches(final int edge) {
        final int code = snapshot.edgeLabelCode(edge);
        return code >= 0 && code < labelFlags.length && labelFlags[code];
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void onReset() {
        predicate = (P<Object>) node.predicate();
        if (memo != null) memo.clear();
    }

    @Override
    protected void onClose() {
        if (memo != null) memo.close();
        memo = null;
        snapshot = null;
    }
}
