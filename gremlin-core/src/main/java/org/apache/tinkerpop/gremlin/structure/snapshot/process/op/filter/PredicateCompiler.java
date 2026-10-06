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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Preds;

import java.util.Arrays;

/**
 * Binds the {@link Preds} descriptors to an execution: resolves what depends on the snapshot and returns an
 * {@link EntryPredicate} for the lane. The semantics are those of {@code HasContainer} and {@code IsStep}:
 * <ul>
 *     <li>label sets are already evaluated per dictionary code, a multi-label vertex matches if any label does;</li>
 *     <li>identifier predicates test the decoded identifier, or its {@code toString()} for a string test;</li>
 *     <li>a property predicate is true if any value of the key satisfies {@code P.test}, a present null is tested with
 *     {@code test(null)}, an absent property and an unknown key are false;</li>
 *     <li>presence is true for a present null.</li>
 * </ul>
 * {@code P} instances are tested live, so values that were updated between executions are seen without rebinding.
 * No typed fast paths are used, every value goes through the decoded {@code P.test}.
 */
public final class PredicateCompiler {

    private PredicateCompiler() {
    }

    /**
     * @param pred the predicate
     * @param lane the lane of the batches it will be evaluated on
     * @throws IllegalArgumentException if the predicate does not apply to the lane
     */
    public static EntryPredicate compile(final Preds.Pred pred, final CsrExecutionContext ctx, final Lane lane) {
        final CsrSnapshot snapshot = ctx.snapshot();
        final CsrGraph graph = ctx.graph();
        if (pred instanceof Preds.LaneType laneType) {
            final boolean result = laneType.lane() == lane;
            return (batch, i) -> result;
        }
        if (pred instanceof Preds.LabelSet labelSet) return labelSet(labelSet.byCode(), snapshot, lane);
        if (pred instanceof Preds.IdIn idIn) return idIn(idIn.ordinals(), lane);
        if (pred instanceof Preds.IdPred idPred) return idPred(idPred, snapshot, lane);
        if (pred instanceof Preds.PropPred propPred) return propPred(propPred, ctx, lane);
        if (pred instanceof Preds.Presence presence) return presence(presence.keyCode(), ctx, lane);
        if (pred instanceof Preds.ValuePred valuePred) return valuePred(valuePred.predicate(), ctx, lane);
        if (pred instanceof Preds.KeyPred keyPred) return keyPred(keyPred.predicate(), graph, lane);
        throw new IllegalArgumentException("No native evaluation for the predicate " + pred);
    }

    private static IllegalArgumentException notFor(final Object pred, final Lane lane) {
        return new IllegalArgumentException(pred.getClass().getSimpleName() + " does not apply to " + lane);
    }

    @SuppressWarnings("unchecked")
    private static boolean test(final P<?> predicate, final Object value) {
        return ((P<Object>) predicate).test(value);
    }

    // ---------------------------------------------------------------- label, id

    private static EntryPredicate labelSet(final boolean[] byCode, final CsrSnapshot snapshot, final Lane lane) {
        if (lane == Lane.E) {
            return (batch, i) -> {
                final int code = snapshot.edgeLabelCode(batch.ord[i]);
                return code >= 0 && code < byCode.length && byCode[code];
            };
        }
        if (lane != Lane.V) throw notFor(Preds.LabelSet.class, lane);
        if (!snapshot.isMultiLabel()) {
            return (batch, i) -> {
                final int code = snapshot.vertexLabelCode(batch.ord[i]);
                return code >= 0 && code < byCode.length && byCode[code];
            };
        }
        return (batch, i) -> {
            final int vertex = batch.ord[i];
            final int count = snapshot.vertexLabelCount(vertex);
            for (int k = 0; k < count; k++) {
                final int code = snapshot.vertexLabelCodeAt(vertex, k);
                if (code >= 0 && code < byCode.length && byCode[code]) return true;
            }
            return false;
        };
    }

    private static EntryPredicate idIn(final int[] ordinals, final Lane lane) {
        if (!lane.isElement()) throw notFor(Preds.IdIn.class, lane);
        final int[] sorted = ordinals.clone();
        Arrays.sort(sorted);
        return (batch, i) -> Arrays.binarySearch(sorted, batch.ord[i]) >= 0;
    }

    private static EntryPredicate idPred(final Preds.IdPred idPred, final CsrSnapshot snapshot, final Lane lane) {
        final P<?> predicate = idPred.predicate();
        final boolean string = idPred.stringTest();
        switch (lane) {
            case V:
                return (batch, i) -> testId(predicate, string, snapshot.vertexId(batch.ord[i]));
            case E:
                return (batch, i) -> testId(predicate, string, snapshot.edgeId(batch.ord[i]));
            case VP:
                return (batch, i) -> testId(predicate, string,
                        snapshot.vertexPropertyIdentifier(batch.key[i], batch.aux[i]));
            default:
                throw notFor(idPred, lane);
        }
    }

    private static boolean testId(final P<?> predicate, final boolean string, final Object id) {
        return test(predicate, string ? id.toString() : id);
    }

    // ---------------------------------------------------------------- properties

    private static EntryPredicate propPred(final Preds.PropPred propPred, final CsrExecutionContext ctx,
                                           final Lane lane) {
        final int keyCode = propPred.keyCode();
        final P<?> predicate = propPred.predicate();
        final CsrSnapshot snapshot = ctx.snapshot();
        if (lane == Lane.V) {
            if (keyCode < 0) return (batch, i) -> false;
            return (batch, i) -> {
                final int vertex = batch.ord[i];
                final long end = snapshot.vertexPropertyEnd(keyCode, vertex);
                for (long p = snapshot.vertexPropertyStart(keyCode, vertex); p < end; p++) {
                    if (test(predicate, snapshot.vertexPropertyValue(keyCode, p))) return true;
                }
                return false;
            };
        }
        if (lane == Lane.E) {
            final ColumnReader column = keyCode < 0 ? null : ctx.graph().edgeColumn(keyCode);
            if (column == null) return (batch, i) -> false;
            return (batch, i) -> {
                final long entry = column.entryIndex(batch.ord[i]);
                return entry >= 0 && test(predicate, column.getAt(entry));
            };
        }
        throw notFor(propPred, lane);
    }

    private static EntryPredicate presence(final int keyCode, final CsrExecutionContext ctx, final Lane lane) {
        final CsrSnapshot snapshot = ctx.snapshot();
        if (lane == Lane.V) {
            if (keyCode < 0) return (batch, i) -> false;
            return (batch, i) -> snapshot.vertexPropertyEnd(keyCode, batch.ord[i])
                    > snapshot.vertexPropertyStart(keyCode, batch.ord[i]);
        }
        if (lane == Lane.E) {
            final ColumnReader column = keyCode < 0 ? null : ctx.graph().edgeColumn(keyCode);
            if (column == null) return (batch, i) -> false;
            return (batch, i) -> column.entryIndex(batch.ord[i]) >= 0;
        }
        throw new IllegalArgumentException("Presence does not apply to " + lane);
    }

    // ---------------------------------------------------------------- values and keys

    private static EntryPredicate valuePred(final P<?> predicate, final CsrExecutionContext ctx, final Lane lane) {
        final CsrSnapshot snapshot = ctx.snapshot();
        final CsrGraph graph = ctx.graph();
        switch (lane) {
            case VAL:
            case SCALAR:
                return (batch, i) -> test(predicate, Materializer.value(ctx, batch, i));
            case VP:
                return (batch, i) -> test(predicate, snapshot.vertexPropertyValue(batch.key[i], batch.aux[i]));
            case EP:
                return (batch, i) -> test(predicate, graph.edgeColumn(batch.key[i]).get(batch.ord[i]));
            case MP:
                return (batch, i) -> test(predicate,
                        snapshot.metaPropertyValue(batch.key[i], batch.src[i], batch.aux[i]));
            default:
                throw new IllegalArgumentException("ValuePred does not apply to " + lane);
        }
    }

    private static EntryPredicate keyPred(final P<?> predicate, final CsrGraph graph, final Lane lane) {
        switch (lane) {
            case VP:
                return (batch, i) -> test(predicate, graph.vertexKey(batch.key[i]));
            case EP:
                return (batch, i) -> test(predicate, graph.edgeKey(batch.key[i]));
            case MP:
                return (batch, i) -> test(predicate, graph.metaKey(batch.src[i]));
            default:
                throw new IllegalArgumentException("KeyPred does not apply to " + lane);
        }
    }
}
