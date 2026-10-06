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

import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

import java.util.List;

/**
 * {@code g.V(ids)} and {@code g.E(ids)}: the identifiers are resolved to ordinals when the operator opens, in argument
 * order with duplicates; null identifiers and identifiers that are not in the snapshot are skipped.
 */
final class LookupOperator extends AbstractCsrOperator {

    private final Lane lane;
    private final List<Object> ids;
    private int[] ordinals;
    private long reserved;
    private int next;

    LookupOperator(final Sources.Lookup node, final OperatorSpec spec) {
        super(spec);
        this.lane = node.lane();
        this.ids = node.ids();
    }

    @Override
    protected void doOpen() {
        final CsrSnapshot snapshot = ctx.snapshot();
        reserved = 4L * ids.size();
        ctx.budget().reserve(reserved, owner("ordinals"));
        final int[] resolved = new int[ids.size()];
        int n = 0;
        for (final Object id : ids) {
            if (id == null) continue;
            final int ordinal = lane == Lane.V ? snapshot.vertexOrdinalCoerced(id) : snapshot.edgeOrdinalCoerced(id);
            if (ordinal >= 0) resolved[n++] = ordinal;
        }
        ordinals = n == resolved.length ? resolved : java.util.Arrays.copyOf(resolved, n);
        next = 0;
    }

    @Override
    protected boolean produce(final Batch out) {
        while (next < ordinals.length && !out.isFull()) {
            out.ord[out.n] = ordinals[next++];
            out.bulk[out.n++] = 1L;
        }
        return next < ordinals.length;
    }

    @Override
    protected void doReset() {
        next = 0;
    }

    @Override
    protected void doClose() {
        ordinals = null;
        if (reserved > 0) ctx.budget().release(reserved, owner("ordinals"));
        reserved = 0;
    }
}
