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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * {@code out}, {@code in}, {@code both}, {@code outE}, {@code inE}, {@code bothE}: walks the adjacency ranges of each
 * input vertex. {@code BOTH} walks the out range, then the in range, so a self-loop appears twice. The operator keeps
 * the input vertex, the direction phase and the adjacency position between calls, so a hub vertex never produces more
 * than one output batch at a time. Edge labels are resolved to a flag per label code when the operator opens; a label
 * that is not in the dictionary was dropped by the planner and matches nothing.
 */
final class ExpandOperator extends AbstractCsrOperator {

    private final Direction direction;
    private final int[] labelCodes;
    private final boolean emitEdges;
    private final boolean recordSource;

    private CsrSnapshot snapshot;
    private boolean[] labelFlags;
    private boolean nothing;
    private Batch in;
    private int index;
    private boolean active;
    private int vertex;
    private long bulk;
    private boolean outPhase;
    private boolean inPending;
    private long position;
    private long end;

    ExpandOperator(final Ops.Expand node, final OperatorSpec spec) {
        super(spec);
        this.direction = node.direction();
        this.labelCodes = node.labelCodes();
        this.emitEdges = node.emit() == Lane.E;
        this.recordSource = node.recordSource();
    }

    @Override
    protected void doOpen() {
        snapshot = ctx.snapshot();
        in = spec.newInputBatch(ctx.batchSize());
        nothing = labelCodes != null && labelCodes.length == 0;
        labelFlags = null;
        if (labelCodes != null && labelCodes.length > 0) {
            labelFlags = new boolean[snapshot.edgeLabels().size()];
            for (final int code : labelCodes) {
                if (code >= 0 && code < labelFlags.length) labelFlags[code] = true;
            }
        }
        restart();
    }

    private void restart() {
        in.n = 0;
        index = 0;
        active = false;
    }

    @Override
    protected boolean produce(final Batch out) {
        if (nothing) {
            // nothing to emit, but the input is consumed so that upstream side effects happen as they would otherwise
            while (pull(in)) ctx.checkInterrupt();
            return false;
        }
        while (true) {
            if (active) {
                if (emit(out)) return true;
                active = false;
                index++;
            }
            if (index >= in.n) {
                if (!pull(in)) return false;
                index = 0;
                continue;
            }
            begin(index);
            ctx.checkInterrupt();
        }
    }

    private void begin(final int i) {
        vertex = in.ord[i];
        bulk = in.bulk[i];
        active = true;
        switch (direction) {
            case OUT:
                outPhase = true;
                inPending = false;
                break;
            case IN:
                outPhase = false;
                inPending = false;
                break;
            default:
                outPhase = true;
                inPending = true;
                break;
        }
        if (outPhase) {
            position = snapshot.outStart(vertex);
            end = snapshot.outEnd(vertex);
        } else {
            position = snapshot.inStart(vertex);
            end = snapshot.inEnd(vertex);
        }
    }

    /**
     * Emits the current vertex's remaining adjacency.
     *
     * @return true if the output batch is full and the vertex has more to emit
     */
    private boolean emit(final Batch out) {
        while (true) {
            if (outPhase) {
                for (long p = position; p < end; p++) {
                    final boolean needEdge = emitEdges || labelFlags != null;
                    final int edge = needEdge ? snapshot.outEdge(p) : -1;
                    if (labelFlags != null && !matches(edge)) continue;
                    if (out.isFull()) {
                        position = p;
                        return true;
                    }
                    if (emitEdges) {
                        if (recordSource) out.addE(edge, vertex, bulk);
                        else out.addE(edge, bulk);
                    } else {
                        out.addV(snapshot.outNeighbor(p), bulk);
                    }
                }
            } else {
                for (long p = position; p < end; p++) {
                    final boolean needEdge = emitEdges || labelFlags != null;
                    final int edge = needEdge ? snapshot.inEdge(p) : -1;
                    if (labelFlags != null && !matches(edge)) continue;
                    if (out.isFull()) {
                        position = p;
                        return true;
                    }
                    if (emitEdges) {
                        if (recordSource) out.addE(edge, vertex, bulk);
                        else out.addE(edge, bulk);
                    } else {
                        out.addV(snapshot.inNeighbor(p), bulk);
                    }
                }
            }
            if (outPhase && inPending) {
                outPhase = false;
                inPending = false;
                position = snapshot.inStart(vertex);
                end = snapshot.inEnd(vertex);
            } else {
                return false;
            }
        }
    }

    private boolean matches(final int edge) {
        final int code = snapshot.edgeLabelCode(edge);
        return code >= 0 && code < labelFlags.length && labelFlags[code];
    }

    @Override
    protected void doReset() {
        restart();
    }

    @Override
    protected void doClose() {
        in = null;
        snapshot = null;
        labelFlags = null;
    }
}
