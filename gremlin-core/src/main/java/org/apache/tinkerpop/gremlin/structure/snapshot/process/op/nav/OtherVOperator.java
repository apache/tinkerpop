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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;

/**
 * {@code otherV}: the end vertex of the edge that is not the vertex the edge was reached from, which the upstream
 * {@code Expand} recorded in the batch. For a self-loop both ends are the same vertex.
 */
final class OtherVOperator extends EntryStreamOperator {

    private CsrSnapshot snapshot;

    OtherVOperator(final OperatorSpec spec) {
        super(spec);
        if (!spec.inputRecordsSource()) {
            throw new IllegalStateException("otherV() needs edges that recorded the vertex they were reached from");
        }
    }

    @Override
    protected void onOpen() {
        snapshot = ctx.snapshot();
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        final int edge = in.ord[i];
        final int outV = snapshot.edgeOut(edge);
        out.addV(outV == in.src[i] ? snapshot.edgeIn(edge) : outV, in.bulk[i]);
        return true;
    }

    @Override
    protected void onClose() {
        snapshot = null;
    }
}
